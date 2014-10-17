import sys

action = sys.argv[1];
language = sys.argv[2];

configfile = '/root/rabbit_config';
lines = open(configfile).readlines();

try:
    languages = lines[0].split()[1].split(',');
except IndexError:
    languages = [];

if action == 'remove':
    languages.remove(language);
elif action == 'add':
    languages.append(language);

lines[0] = 'languages '+','.join(languages) + '\n';
open(configfile,'w').writelines(lines);

