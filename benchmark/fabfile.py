from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from aws.instance import InstanceManager
from aws.remote import Bench, BenchError


@task
def local(ctx):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'nodes': [4],
        'rate': [15_000],
        'tx_size': 512,
        'faults': 0,
        'workers': 1,
        'duration': 8,
        'runs': 1,
        'co-locate': True,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 1_000,
            'sync_retry_delay': 1_000,
            'max_payload_size': 7_812_500,
            'min_block_delay': 0,
            'simulate_asynchrony': False,
            'async_type': [3],
            'asynchrony_start': [10_000],
            'asynchrony_duration': [20_000],
            'affected_nodes': [2],
            'egress_penalty': 50,
            'use_exponential_timeouts': False,
        },
        'mempool': {
            'queue_capacity': 10_000_000,
            'sync_retry_delay': 1_000,
            'max_payload_size': 500_000,
            'min_block_delay': 0
        }
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug=False).result()
        print(ret)
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=1):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install HotStuff on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx):
    ''' Run benchmarks on GCP '''
    bench_params = {
        'nodes': [4],
        # Hao: seems only autobahn and bullshark uses it, but I add it for collocate 
        'workers': 1,
        # ran: 12_500, 15_000, 10_000, 20_000,3_000,30_000,5_000,50_000,7_500, 100_000, 150_000,200_000, 300_000, 500_000, 1_000_000; 
        # to be ran:
        'rate': [5_000],
        # Hao: 9 is the minimal 
        'tx_size': 9,
        'faults': 0,
        'duration': 40,
        'runs': 1,
        'co-locate': True,
    }
    node_params = {
        'consensus': {
            'timeout_delay': 5_000,
            'sync_retry_delay': 5_000,
            'max_payload_size': 7_812_500,
            'min_block_delay': 0,
            'simulate_asynchrony': False,
            'async_type': [2],
            'asynchrony_start': [25_000],
            'asynchrony_duration': [1_000],
            'affected_nodes': [1],
            'egress_penalty': 0,
            'use_exponential_timeouts': True,
        },
        'mempool': {
            'queue_capacity': 10_000_000,
            'sync_retry_delay': 5_000,
            'max_payload_size': 500_000,
            'min_block_delay': 0
        }
    }
    try:
        print(bench_params)
        Bench(ctx).run(bench_params, node_params, debug=True)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'nodes': [4],
        'tx_size': 9,
        'faults': [0],
        'max_latency': [2_000, 5_000],
        'collocate': True,
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop any HotStuff execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
