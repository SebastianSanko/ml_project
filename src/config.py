# +

from keyring_pass import PasswordStoreBackend
from getpass import getpass
from os import getenv
import os
import keyring
import hvac
import configparser


# -

# Please initialize the keyring in the SANDBOX environment before use (see /home/jovyan/utils !)

def get_properties():
    """
    Function returns credentials depending on the environment using keyring (Password Store) or Vault .

    Returns:
        dict: Credentials saved in Password Store or Vault 
    """
    work_env = getenv("WORK_ENV", "SANDBOX")

    if work_env == "SANDBOX":
        
         # Get keyring backend
        kr = PasswordStoreBackend()
        keyring.set_keyring(kr)

        return list_credentials_from_cfg('/home/jovyan/utils/credential_keys.cfg')
    
    elif work_env == "PROD":

        client = hvac.Client(url=os.getenv("VAULT_URL"), cert=(os.getenv('VAULT_CERT_PATH')),verify=False)
        client.login("/v1/auth/cert/login")
        if not client.is_authenticated():
          error_msg = 'Unable to authenticate to the Vault service'
          raise hvac.exceptions.Unauthorized(error_msg)
        
        response = get_password_buckets(client, os.getenv('VAULT_CFG_PATH'))
                
        return response

def list_credentials_from_cfg(file_path):
    config = configparser.ConfigParser(allow_no_value=True)
    config.optionxform = str
    config.read(file_path)
    config_data = {}
    for service in config.sections():
        for key in config[service]:

            if config[service].get(key, None) is None:
                password = keyring.get_password(service, key)
            else:
                password = config[service].get(key, None)

            config_data.update({key : password})

    return config_data


# Use case
# credentials = get_properties()
# print(credentials)

# +
def get_password_buckets(client, file_path):
    config = configparser.ConfigParser(allow_no_value=True)
    config.optionxform = str
    config.read(file_path)
    config_data = {}
    for service in config.sections():
        for key in config[service]:

            read_response = client.secrets.kv.v2.read_secret(mount_point='system', path=f'{os.getenv("VAULT_SECRETS_PATH")}/{key}')
            config_data.update(read_response['data']['data'])

    return config_data





