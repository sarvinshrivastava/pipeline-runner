#!/usr/bin/env bash
# Vendors swagger-ui-dist into internal/web/static/apidocs/ (served at /docs/).
#
#   scripts/vendor-swagger-ui.sh <version>       vendor a 5.x release at least 14 days old
#   scripts/vendor-swagger-ui.sh --list-eligible print the versions that pass that rule, newest last
#   scripts/vendor-swagger-ui.sh --verify        compare the vendored files with the registry tarball (CI)
#
# Every download is checked against the registry's sha512 integrity. Nothing from the
# tarball is executed, and only the five files below are extracted from it.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
dest="$root/internal/web/static/apidocs"
files=(swagger-ui-bundle.js swagger-ui.css LICENSE NOTICE swagger-ui-bundle.js.LICENSE.txt)

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# registry <mode> [version enforce-age]: list | info (prints tarball URL, then integrity).
registry() {
	python3 -I -c '
import json, re, sys
from datetime import datetime, timedelta, timezone

meta = json.load(open(sys.argv[1]))
mode = sys.argv[2]
cutoff = datetime.now(timezone.utc) - timedelta(days=14)

def published(v):
    t = meta["time"][v]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(t, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    sys.exit("cannot parse publish time %r of %s" % (t, v))

def eligible():
    out = []
    for v, info in meta["versions"].items():
        if not re.fullmatch(r"5\.\d+\.\d+", v) or info.get("deprecated") or v not in meta["time"]:
            continue
        if published(v) <= cutoff:
            out.append(v)
    return sorted(out, key=lambda v: tuple(int(p) for p in v.split(".")))

if mode == "list":
    print("\n".join(eligible()))
else:
    v, enforce = sys.argv[3], sys.argv[4] == "1"
    if v not in meta["versions"]:
        sys.exit("unknown swagger-ui-dist version " + v)
    if enforce and v not in eligible():
        sys.exit(v + " is not eligible: it must be a 5.x release, not deprecated, published at least 14 days ago (see --list-eligible)")
    dist = meta["versions"][v]["dist"]
    print(dist["tarball"])
    print(dist["integrity"])
' "$tmp/meta.json" "$@"
}

# fetch <version> <enforce-age>: download and verify the tarball, extract the five files into $tmp/x/package.
fetch() {
	local info tarball integrity got
	info=$(registry info "$1" "$2")
	tarball=$(sed -n 1p <<<"$info")
	integrity=$(sed -n 2p <<<"$info")
	case "$tarball" in https://registry.npmjs.org/*) ;; *) echo "unexpected tarball URL $tarball" >&2; exit 1 ;; esac
	curl -fsSL "$tarball" -o "$tmp/p.tgz"
	got="sha512-$(openssl dgst -sha512 -binary "$tmp/p.tgz" | base64 | tr -d '\n')"
	if [ "$got" != "$integrity" ]; then
		echo "integrity mismatch for $1: registry $integrity, download $got" >&2
		exit 1
	fi
	mkdir "$tmp/x"
	tar -xzf "$tmp/p.tgz" -C "$tmp/x" "${files[@]/#/package/}"
}

curl -fsSL https://registry.npmjs.org/swagger-ui-dist -o "$tmp/meta.json"

case "${1:-}" in
"")
	echo "usage: $0 <version> | --list-eligible | --verify" >&2
	exit 2
	;;
--list-eligible)
	registry list
	;;
--verify)
	version=$(tr -d '[:space:]' <"$dest/VERSION")
	fetch "$version" 0
	for f in "${files[@]}"; do
		if ! cmp -s "$tmp/x/package/$f" "$dest/$f"; then
			echo "vendored $f differs from swagger-ui-dist@$version" >&2
			exit 1
		fi
	done
	echo "vendored Swagger UI matches swagger-ui-dist@$version"
	;;
*)
	version=$1
	fetch "$version" 1
	for f in "${files[@]}"; do
		cp "$tmp/x/package/$f" "$dest/$f"
	done
	printf '%s\n' "$version" >"$dest/VERSION"
	python3 -I -c '
import re, sys
path, v = sys.argv[1], sys.argv[2]
s = open(path).read()
s, n = re.subn(r"(/docs/swagger-ui(?:-bundle\.js|\.css)\?v=)[0-9A-Za-z.-]+", lambda m: m.group(1) + v, s)
if n != 2:
    sys.exit("expected 2 ?v= references in %s, found %d" % (path, n))
open(path, "w").write(s)
' "$dest/index.html" "$version"
	echo "vendored swagger-ui-dist@$version; record these in docsVendorSHA256:"
	(cd "$dest" && shasum -a 256 swagger-ui-bundle.js swagger-ui.css)
	echo "DOMPurify: $(grep -o 'DOMPurify.version="[^"]*"' "$dest/swagger-ui-bundle.js" | head -1); .sanitize( calls: $(grep -o '\.sanitize(' "$dest/swagger-ui-bundle.js" | wc -l | tr -d ' ')"
	;;
esac
