import logging
import pytest

from ocs_cs.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier1, accepatnce
)
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources import snapshot
from tests import helpers

log = logging.getLoggete(__name__)

@tier1
@skipif_ocs_version('<4.6')
@pytest.mark.parametrize(
    argnames=["interface_type"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("")
        )
    ]
)
class TestPvcSnapshot(ManageTest):
    """
    Tests to verify PVC snapshot feature
    """
    @pytest.fixture(autouse=True)
    def setup(self, interface_type, storageclass_factory, pvc_factory, pod_factory):
        """
        create resources for the test
        Args:
            interface_type(str): The type of the interface
                (e.g. CephBlockPool, CephFileSystem)
            storageclass_factory: A fixture to creare new storage class
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod
        """
        self.sc_obj = storageclass_factory(interface=interface_type)
        self.pvc_obj = pvc_factory(
            interface=interface_type,
            size=5
            status=constants.STATUS_BOUND
        )
        self.pod_obj = pod_factory(
            interface=interface_type,
            pvc=self.pvc_obj_rbd,
            status=constants.STATUS_RUNNING
        )
        # Create snapshot class object
        logger.info(f"Creating a snapshot storage class")
        self.snapshot_sc_data = templating.load_yaml(
            constants.CSI_RBD_SNAPSHOTCLASS_YAML
        )
        self.snapshot_sc_data['metadata']['name'] = helpers.create_unique_resource_name(
            'test', 'csi-rbd'
        )
        self.snapshot_sc_obj = OCS(**self.sc_data)
        assert snapshot_sc_obj.create()
        logger.info(f"Snapshot class: {snapshot_sc_obj.name} created successfully")

    def test_pvc_snapshot(self):
        """
        Create/Delete snapshot,
        verify data is preserved after taking a snapshot
        """
        logger.info(f"Running IO on pod {pod_obj.name}")
        file_name = pod_obj.name
        logger.info(file_name)
        pod_obj.run_io(
            storage_type='fs', size='1G', fio_filename=file_name
        )

        # Verfiy presence of the file
        file_path = pod.get_file_path(pod_obj, file_name)
        assert pod.check_file_existence(pod_obj, file_name), (
            f"File {file_name} doesn't exist"
        )
        logger.info(f"File {file_name} exists in {pod_obj.name}")

        # Take a snapshot
        snapshot_obj = helpers.create_pvc_snapshot(
            snapshot_sc_name=self.snapshot_sc_obj.name,
            pvc_name=self.pvc_obj.name
        )
        helpers.wait_for_resource_state(
            snapshot_obj,
            state=constants.STATUS_READYTOUSE,
            timeout=60
        )

        # Create pvc out of the snapshot
        restore_pvc_obj = helpers.create_restore_pvc(
            sc_name=self.sc_obj.name, snap_name=snapshot_obj.name,
            size=5
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        resource_pvc_obj.reload()

        # Create and attach pod to the pvc
        restore_pod_obj = pod_factory(
            interface=interface_type,
            pvc=restore_pvc_obj,
            status=constants.STATUS_RUNNING
        )

        # Verify file present on the new pod
        logger.info(f"Checking the existence of {file_name} on restore pod {restore_pod_obj.name}")
        assert pod.check_file_existence(restore_pod_obj, file_name), (
                f"File {file_name} doesn't exist"
            )
        logger.info(f"File {file_name} exists in {restore_pod_obj.name}")

        # Delete the snapshot
        assert snapshot_obj.delete()
        snapshot_obj.ocp.wait_for_delete(resource_name=snapshot_obj.name)

        # Delete the restored pod
        assert restore_pod_obj.delete()
        restore_pod_obj.ocp.wait_for_delete(resource_name=restore_pod_obj.name)

        # Delete the pvc created from snapshot
        assert restore_pvc_obj.delete()
        restore_pvc_obj.ocp.wait_for_delete(resource_name=restore_pvc_obj.name)

