import inspect as _inspect
import functools
from . import job, dispatch, schedulers

__all__ = [ 'PicklableLambda', 'islambda', 'map', 'partial']


# ----------------------------------------------------------------------------
# Partial
# ----------------------------------------------------------------------------
def isexception(x):
    """Test whether the value is an Exception instance"""
    return isinstance(x, Exception)


def isnumeric(x):
    """Test whether the value can be represented as a number"""
    try:
        float(x)
        return True
    except:
        return False


def islambda(func):
    """ Test if the function func is a lambda ("anonymous" function) """
    return getattr(func, 'func_name', False) == '<lambda>'


class PicklableLambda(object):
    """ Class/Decorator that ensures a lambda ("anonymous" function) will be
    picklable.  Lambda are not picklable because they are anonymous while
    pickling mainly works with the names.  This class digs out the code of the
    lambda, which is picklable and recreates the lambda function when called.
    The encapsulated lambda is not anonymous anymore.

    Notes:
        * Dependencies are not handled.
        * Often Partial can replace a lambda definition
    """
    def __init__(self, func):
        if not islambda(func):
            raise TypeError('Object not a lambda function')
        self.func_code = _inspect.getsource(func)
        self.__name__ = self.func_code.split('=')[0].strip()

    def __repr__(self):
        return self.func_code + object.__repr__(self)

    def __call__(self, *args, **kwargs):
        func = eval(self.func_code.split('=')[1])
        return func(*args, **kwargs)


def partial(f, *args, **kwargs):
    """Return a callable partially closed over the input function and arguments

    partial is functionally equivalent to functools.partial, however it also
    applies a variant of functools.update_wrapper, with:

        __doc__    = f.__doc__
        __module__ = f.__module__
        __name__   = f.__name__ + string_representation_of_closed_arguments

    This is useful for running functions with different parameter sets, whilst
    being able to identify the variants by name
    """
    def name(var):
        try:
            return var.__name__
        except AttributeError:
            return str(var)[0:5] if isnumeric(var) else var.__class__.__name__
    if islambda(f):
        g = functools.partial(PicklableLambda(f), *args, **kwargs)
    else:
        g = functools.partial(f, *args, **kwargs)
    g.__doc__ = f.__doc__
    g.__module__ = f.__module__
    g.__name__ = '_'.join([f.__name__] + [name(arg) for arg in list(args) +
                                          list(kwargs.values())])
    return g


# ----------------------------------------------------------------------------
# Map
# ----------------------------------------------------------------------------
def map(f, args, scheduler=schedulers.best_available, reraise=True):
    """Perform a functional-style map operation

    Apply a function f to each argument in the iterable args. This is equivalent to
        y = [f(x) for x in args]
    or
        y = map(f, args)
    except that each argument in the iterable is assigned to a separate Job
    and scheduled to run via the scheduler.

    The default scheduler is a schedulers.ProcessScheduler instance. To run map
    on a grid engine, simply pass a schedulers.GridEngineScheduler instance.

    Parameters
    ----------
    f: callable
        A picklable function
    args: iterable
        An iterable (list) of arguments to f

    scheduler: schedulers.Scheduler instance or class
        By default, the system tries to return the best_available() scheduler.
        Use this if you want to set a scheduler specifically.

    reraise: bool
        Reraise exceptions that occur in any of the jobs. Set this to False if
        you want to salvage any good results.

    Returns
    -------
        List of return values equivalent to the builtin map function

    Raises
    ------
        Any exception that would occur when applying [f(x) for x in args]
    """

    # setup the dispatcher
    dispatcher = dispatch.JobDispatcher(scheduler)

    # allocate the jobs
    jobs = [job.Job(target=f, args=(arg,)) for arg in args]

    # run the jobs (guaranteed to return in the same order)
    dispatcher.dispatch(jobs)
    results = dispatcher.join()

    # check for exceptions
    if reraise:
        for exception in filter(isexception, results):
            # an error occurred during execution of one of the jobs, reraise it
            raise exception

    return results
