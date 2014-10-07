from distutils.core import setup
setup(name='scalator',
      version='1.0',
      py_modules=['scalator.scalatorcmd', 'scalator.scalatord', 'scalator.scalator'],
      data_files=[('/var/lib/scalator/', ['scalator/templates/rabbit_config'])])

