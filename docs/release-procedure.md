Dask-jobqueue heavily relies on dask and distributed upstream projects.  
We may want to check their status while releasing.


Release for dask-jobqueue, from within your fork:

* Submit a PR that updates the release notes in `docs/source/changelog.rst`. 
We submit a PR to inform other developers of the pending release, and possibly
discuss its content.

* Once the PR is merged, checkout the master branch:

````
git checkout upstream/master
````

* Create a tag and push to github:

````
git tag -a x.x.x -m 'Version x.x.x'
git push --tags upstream
````

* Build the wheel/dist and upload to PyPI:

````
git clean -xfd
python setup.py sdist bdist_wheel --universal
twine upload dist/*
````

* The Conda Forge bots should pick up the change automatically within an hour
or two. Then follow the instructions from the automatic e-mail that you will
receive from Conda Forge, basically:
  * Check that dependencies have not changed.
  * Merge the PR on conda-forge/dask-jobqueue-feedstock once tests have passed.

