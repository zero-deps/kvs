# Release

To create new release:

* `release=??? bash -c 'git checkout master && git push && git tag -a $release -m "release $release" && git push origin $release'`
* `sbt publish`
