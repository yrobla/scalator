Revisely autoscaling tools
==========================

This readme is written using Markdown syntax. Although it should be pretty well readable in bare form, it might be even easier to read from a Markdown parser, like http://dillinger.io . 

1. Installation
---------------

Revisely's autoscaling tools are written in Python 2, and run almost(!) out of the box on any Linux system by simply typing:

```
python <script_name>
```

However, there are a number of Python libraries that might not be installed, namely:

* __skiff__ v0.9.6, to be installed with ```sudo pip install skiff==0.9.6```, used for communication with DigitalOcean.
* __requests__, to be installed with ```sudo pip install requests```, also used for communication with DigitalOcean.
* __pysftp__, to be installed with ```sudo pip install pysftp```, used to log in on servers.

If you do not have the installation tool pip installed, do:

```
sudo apt-get install python-pip
```

2. Starting a Revelator
-----------------------

This script activates a Droplet, using the Digital Ocean API. Furthermore, it automatically sets the settings of the Droplet to what is needed for a Revelator (like activating all public keys). The only thing left for you to do is:

```
python create_revelator.py <name> <size> <snapshot_version>
```

Let us go through these arguments one by one:
* The name of the Revelator. This can contain only letters. The script will put 'rev.' in front of the name automatically, to to distinguish the droplet from other Revisely droplets.
* The size of the Revelator, expressed in MB/GB RAM. You can pick from all Droplet sizes, as seen here: https://www.digitalocean.com/pricing/
* Which Revelator snapshot to use. If you leave out this argument, and have an up-to-date version of the script, the latest version will be used automatically.

An example:

```
python create_revelator.py test 32GB 10
```

It usually takes about 1-2 minutes to start a Revelator. For questions and suggestions for this script, please go to: https://revise.ly/redmine/issues/288

3. Tranforming a Revelator
--------------------------

This script activates and deactivates spelling correctors (or tokenizers for Spanish) and all needed background server software, as well the RabbitMQ receiver script, which makes sure the Revelator listens to the correct queue. RabbitMQ itself makes sure the work is split evenly between all Revelators listening to a particular queue.

It is activated like this

```
python transform_revelator.py <arg-1> <arg-2> <arg-n>
```

An argument is consists of three parts:
* A + for activating things or a - for deactivating things.
* A letter indicating _what_ you're trying to active - only the l for language is activated right now.
* A colon, :.
* A three-letter code indicating the language:
   * eng: English
   * nld: Dutch
   * spa: Spanish

For example, this activates both Valkuil and Fowlt:

```
python transform_revelator.py +l:nld +l:eng
```

And this replaces English with Spanish:

```
python transform_revelator.py -l:eng +l:esp
```

It takes between 2 - 60 seconds to activate a corrector. For questions and suggestions for this script, please go to: https://revise.ly/redmine/issues/287

4. Stopping a Revelator
-----------------------

This script destroys a Revelator if it has nothing to do. It works like this:

```
python fire_revelator.py <revelator_name>
```

Notes:
* This works only for Revelator snapshots version 11 and higher
* This procedure was set up in such a way that it will only stop if it has been doing nothing for 1 hour, otherwise the Revelator will ignore the command. In other words, you're free to run this command as often as you want, as it will only work if destroying the Revelator won't hurt anyone; the only risk is that you destroy __all__ Revelators. For questions and suggestions for this script, please go to: https://revise.ly/redmine/issues/289

5. Misc
-------
* Info about the Revelator snapshot can be found here: https://revise.ly/redmine/issues/65#note-8
