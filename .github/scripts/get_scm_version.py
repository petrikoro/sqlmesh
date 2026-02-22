from setuptools_scm import get_version  # ty:ignore[unresolved-import]

version = get_version(root='../../', relative_to=__file__)
print(version.split('+')[0])
