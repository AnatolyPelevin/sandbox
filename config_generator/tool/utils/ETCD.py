import etcd3
import json
import logging

from os import environ

logging.basicConfig(level=logging.INFO)

etcd_keys = {
    'production': 'RC-PRO::APP.OLD::',
    'att': 'ATT-PRO::APP.OLD::',
    'stage': 'OPS-STAGE::APP.OLD::',
    'uat': 'E2E-UAT::APP.OLD::',
    'dev-eurolab': 'DEV-LAB::APP.OLD::',
}


class ETCD(object):
    def __init__(self, env):
        self.etcd = self.create_etcd_client()
        try:
            self.key = etcd_keys[env]
        except Exception as e:
            logging.error(e)
            logging.error(f"environment parameter is incorrect")
            raise

    @staticmethod
    def create_etcd_client():
        logging.info('Creating etcd client')
        try:
            etcd = etcd3.client(ca_cert=environ.get('BDC_CA_CERT'),
                                cert_key=environ.get('BDC_CERT_KEY'),
                                cert_cert=environ.get('BDC_CERT_CERT'),
                                user=environ.get('BDC_USER'),
                                password=environ.get('BDC_PASS'),
                                port=environ.get('BDC_PORT'),
                                host=environ.get('BDC_LBR'))
            return etcd
        except Exception as e:
            logging.error(e)
            raise Exception(f"Connection to {environ.get('BDC_LBR')} etcd failed")

    def get_var(self, var_name):
        key = self.key + var_name
        try:
            logging.info(f"Get {key} variable from etcd")
            result = self.etcd.get(key)
        except Exception as e:
            logging.error(e)
            raise
        value = json.loads(result[0])
        return value
