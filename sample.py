import configparser
from configparser import ConfigParser
config = configparser.ConfigParser()
config['dev']={'database':'retail_db',
                    'host': 'ms.itversity.com',
                    'user_name': 'retail_user',
                    'password':'itversity',
                    'Port': 3306}
with open('sample1.ini', 'w') as configfile:
    config.write(configfile)


