from ..environment_variables import monkey_available

if monkey_available('RPC_BACKEND'):
    from . import rpc  # noqa
    rpc.__package__
