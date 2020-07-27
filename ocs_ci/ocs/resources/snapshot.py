"""
General SNAP object
"""
import logging
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnavailableResourceException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class SNAP(OCS):
    """
    A basic VolumeSnapshot kind resource
    """

    def __init__(self, **kwargs):
        """
        Initializer function
        kwargs:
            See parent class for kwargs information
        """
        super(SNAP, self).__init__(**kwargs)

    @property
    def status(self):
        """
        Returns the SNAP status

        Returns:
            str: SNAP status
        """
        return self.data.get('status').get('phase')

    @property
    def backed_pvc(self):
        """
        Returns the backed PVC name in namespace

        Returns:
            str: PVC name
        """
        return self.data.get('spec').get('persistentVolumeClaimName')

    @property
    def backed_pv_obj(self):
        """
        Returns the backed PVC object in namespace

        Returns:
            OCS: An OCS instance for PVC
        """
        self.reload()
        data = dict()
        data['api_version'] = self.api_version
        data['kind'] = 'PersistentVolumeiClaim'
        data['metadata'] = {
            'name': self.pvc_name, 'namespace': self.namespace
        }
        pvc_obj = OCS(**data)
        pvc_obj.reload()
        return pvc_obj

