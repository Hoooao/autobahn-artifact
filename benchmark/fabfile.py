# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.gcp_instance import InstanceManager
from benchmark.remote import Bench, BenchError


@task
def local(ctx, debug=True):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0, 
        'nodes': 4,
        'workers': 1,
        'rate': 50_000,
        'tx_size': 512,
        'duration': 20,

        # Unused
        'simulate_partition': True,
        'partition_start': 5,
        'partition_duration': 5,
        'partition_nodes': 1,
    }
    node_params = {
        'timeout_delay': 1_000,  # ms
        'header_size': 32,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 1_000,  # ms
        'sync_retry_nodes': 4,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'use_optimistic_tips': False,
        'use_parallel_proposals': True,
        'k': 1,
        'use_fast_path': True,
        'fast_path_timeout': 200,
        'use_ride_share': False,
        'car_timeout': 2000,

        'simulate_asynchrony': True,
        'asynchrony_type': [3],

        'asynchrony_start': [10_000], #ms
        'asynchrony_duration': [20_000], #ms
        'affected_nodes': [2],
        'egress_penalty': 50, #ms

        'use_fast_sync': True,
        'use_exponential_timeouts': True,
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
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
def start(ctx, max=4):
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
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=True):
    ''' Run benchmarks on AWS '''
    # Hao: configed according to paper-results/main-graph/Autobahn/bench-0-4-1-True-200000-512.txt
    bench_params = {
        'faults': 0,
        'nodes': [4],
        'workers': 1,
        'co-locate': True,
        'rate': [200_000],
        'tx_size': 512,
        'duration': 50,
        'runs': 1,

        # Unused
        'simulate_partition': True,
        'partition_start': 5,
        'partition_duration': 5,
        'partition_nodes': 1,
    }
    node_params = {
        'timeout_delay': 1_000,  # ms
        'header_size': 32,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 1_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'use_optimistic_tips': False,
        'use_parallel_proposals': True,
        'k': 1,
        'use_fast_path': True,
        'fast_path_timeout': 200,
        'use_ride_share': False,
        'car_timeout': 2000,

        'simulate_asynchrony': True,
        'asynchrony_type': [3],

        'asynchrony_start': [0], #ms
        'asynchrony_duration': [0], #ms
        'affected_nodes': [0],
        'egress_penalty': 0, #ms

        'use_fast_sync': True,
        'use_exponential_timeouts': True,
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [4],
        'workers': [1, 4, 7, 10],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [2_000, 2_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
