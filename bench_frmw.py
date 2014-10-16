#!/usr/bin/env python
import yaml
import pprint

from lib.classes import Workload, DBClient, YCSB

def parse_config(cfg):
    def load_ycsb(name, wl, opts):
        return wl.get(name, opts.get(name, None))

    config = {}
    cfg = yaml.load(open(cfg, 'r').read())
    config['operators'] = cfg.get('operators', [])
    config['options']   = cfg.get('options', {})
    config['workloads'] = []
    config['retries'] = config['options'].get('retries', 3)
    for name, options in cfg.get('workloads', {}).iteritems():
        # Prepare arguments for Workload
        description = ( # Description is (Full name, Short Name)
            load_ycsb('description', options, config['options']),
            load_ycsb('short_name' , options, config['options'])
        )
        wl_type = load_ycsb('type', options, config['options'])
        threads = load_ycsb('threads', options, config['options'])
        args = config['options'].get('ycsb_parameters', {}).copy()
        args.update(options.get('ycsb_parameters', {}))
        # Create and Populate Workload
        config['workloads'].append(
            Workload(name, wl_type, threads, description, args)
        )
    config['databases'] = []
    for name, db_conf in cfg.get('databases', {}).iteritems():
        cfg_new = config['options'].get('database_parameters', {}).copy()
        cfg_new.update(db_conf)
        config['databases'].append(
            DBClient(
                name,
                cfg_new['server_host'], cfg_new['server_port'],
                cfg_new['db_port'], cfg_new['db_type'],
                cfg_new['description']
            )
        )
    ycsb_path = cfg['directories']['ycsb']
    ycsb_other = cfg['directories']
    del ycsb_other['ycsb']
    if 'exportfile' in config['options'].get('ycsb_parameters', {}):
        ycsb_other['ycsb_export'] = config['options']['ycsb_parameters']['exportfile']
    config['ycsb'] = YCSB(ycsb_path, **ycsb_other)
    config['output'] = config['options'].get('output', {})
    return config

if __name__ == '__main__':
    config = parse_config('bench.yml')
    for wl in config['workloads']:
        wl.run(config)
