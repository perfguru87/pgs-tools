import logging

def configure_logging(verbose=False):
    if verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level, datefmt='%m-%d %H:%M:%S',
                        format="%(asctime)s,%(msecs)03d %(levelname)8s [%(module)s@%(lineno)d] - %(message)s")