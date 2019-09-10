import os
import signal
from kpi_pub import service_shutdown, ServiceExit
import pytest


def test_ServiceExit():

    signal.signal(signal.SIGALRM, service_shutdown)

    with pytest.raises(ServiceExit):
        os.kill(os.getpid(), signal.SIGALRM)
