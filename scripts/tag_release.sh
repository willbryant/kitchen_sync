#!/bin/bash
set -e

if [ $# != "1" ]; then
    echo "./scripts/tag_release.sh <version_tag>"
    exit 1
fi

TAG=$1

if ! grep "^$TAG\$" CHANGES.md >/dev/null; then
	echo "Update CHANGES.md before tagging"
	exit 1
fi

echo "#define KS_VERSION \"$TAG\"" >src/version.h

git commit -am "Update changelog for v$TAG"
git tag v$TAG

echo "To release: "
echo "	git push && git push --tags"
echo "Don't forget to create the release on GitHub afterwards."
