import argparse
import os

import yaml
from yacs.config import CfgNode as CN

_C = CN()

# Base config files
_C.base = ['']

_C.tweets_list = ""  # the tweets list path


_C.proxy_ip = CN()
_C.proxy_ip.list_path = "config/proxy_ip_list/Free_Proxy_List.json"  # the proxy ip list path
_C.proxy_ip.iteration = 100  # the iteration of change the proxy ip

# datasets config
_C.datasets = CN()
_C.datasets.company_path = "datasets/Company.dta"  # the company list path
_C.datasets.company_industry_relation_path = "datasets/CompanyIndustryRelation.dta"  # the company industry list path
_C.datasets.deal_path = "datasets/Deal.dta"  # the deal list path

# the twitter api config
_C.tw = CN()
_C.tw.header = CN()
_C.tw.header.x_twitter_active_user = 'yes'
_C.tw.header.x_twitter_auth_type = 'OAuth2Session'
_C.tw.header.x_twitter_client_language = 'en'
_C.tw.header.referer = 'https://twitter.com/'
_C.tw.header.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                       'Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.49'
_C.tw.header.accept = '*/*'
_C.tw.header.x_csrf_token = ''
_C.tw.header.cookie = ''
_C.tw.header.authorization = ''

_C.tw.output_dir = "output"  # the output dir
_C.tw.output_company_columns_names = []
_C.tw.output_deal_columns_names = []

# save frequency
_C.tw.save_frequency = 100

# the linkin config
_C.lk = CN()
_C.lk.cookis = CN()
_C.lk.cookis.li_at = ''
_C.lk.cookis.li_at_expires = ''
_C.lk.cookis.li_at_max_age = ''


def parse_option():
    parser = argparse.ArgumentParser('scrape the tweets', add_help=False)
    parser.add_argument('--cfg', type=str, default="configs/config.yaml", metavar="FILE", help='path to config file')

    args, unparsed = parser.parse_known_args()
    config = get_config(args)

    return config


def _update_config_from_file(config, cfg_file):
    with open(cfg_file, 'r') as f:
        yaml_cfg = yaml.load(f, Loader=yaml.FullLoader)
    for cfg in yaml_cfg.setdefault('BASE', ['']):
        if cfg:
            _update_config_from_file(
                config, os.path.join(os.path.dirname(cfg_file), cfg)
            )
    print('=> merge config from {}'.format(cfg_file))
    config.merge_from_file(cfg_file)


def update_config(config, args):
    config.defrost()
    if args.cfg:
        _update_config_from_file(config, args.cfg)
    config.freeze()


def get_config(args):
    """Get a yacs CfgNode object with default values."""
    # Return a clone so that the defaults will not be altered
    # This is for the "local variable" use pattern
    config = _C.clone()
    update_config(config, args)
    return config
