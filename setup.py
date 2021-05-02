from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='medicationDiversification',
    version='1.0.0',
    # description='A sample Python project',  # Optional
    # long_description=long_description,  # Optional
    # long_description_content_type='text/markdown',  # Optional (see note above)
    # url='https://github.com/pypa/sampleproject',  # Optional
    # author='A. Random Developer',  # Optional
    # author_email='author@example.com',  # Optional
    # keywords='sample, setuptools, development',  # Optional
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    python_requires='>=3.6, <4',
    # install_requires=['peppercorn'],  # Optional

    # If there are data files included in your packages that need to be
    # installed, specify them here.
    # package_data={  # Optional
    #    'sample': ['package_data.dat'],
    #},

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/distutils/setupscript.html#installing-additional-files
    #
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    # data_files=[('my_data', ['data/data_file'])],  # Optional

    entry_points={  # Optional
        'console_scripts': [
            'mdt=mdt.cli:entry_point',
        ],
    },
)
