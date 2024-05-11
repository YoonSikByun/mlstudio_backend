from setuptools import setup, find_packages
from distutils.command.clean import Command
import os
import glob
import shutil

class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    CLEAN_FILES = './build ./dist ./*.pyc ./*.tgz ./*.egg-info, ./*.log'.split(' ')

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        here = os.getcwd()

        for path_spec in self.CLEAN_FILES:
            # Make paths absolute and relative to this path
            abs_paths = glob.glob(os.path.normpath(os.path.join(here, path_spec)))
            for path in [str(p) for p in abs_paths]:
                if not path.startswith(here):
                    # Die if path in CLEAN_FILES is absolute + outside this directory
                    raise ValueError("%s is not a path inside %s" % (path, here))
                print('removing %s' % os.path.relpath(path))
                shutil.rmtree(path)


setup(name='mlstudio_backend',
      version='0.1',
      license='PYS',
      author='PYS',
      author_email='xian',
      description='Python Backend for ML Studio',
      packages=find_packages(exclude=['test', 'build', 'dist', 'docs']),
      include_package_data=True,
      long_description=open('README.md', encoding='utf-8').read(),
      zip_safe=False,
    #   entry_points={
    #         'console_scripts': [
    #               'mlstudio-compose = sampl2_exe.app.batch.sampl_admin_app:main',
    #         ]
    #   },
      cmdclass={'clean': CleanCommand},
      install_requires=['jinja2', 'pandas', 'numpy', 'sklearn', 'requests', 'gunicorn',
                        'flask', 'flask-restful', 'werkzeug', 'flask-jwt-extended', 'flask-cors',
                        'sqlalchemy', 'six', 'pika', 'fastparquet', 'elasticsearch', 'persistqueue'])
