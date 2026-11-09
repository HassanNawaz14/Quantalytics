import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../ingestion')))

from noaa_collect import utc_timestamp
from qiskit_ibm_runtime.exceptions import IBMInputValueError
import ibm_collect

class TestNOAACollector(unittest.TestCase):
    def test_utc_timestamp_valid(self):
        ts_str, dt = utc_timestamp("2026-09-25T13:33:33Z")
        self.assertEqual(ts_str, "2026-09-25T13:33:33Z")


CRN_GOOD = "crn:v1:bluemix:public:quantum-computing:us-east:a/abc:1111::"
CRN_OTHER = "crn:v1:bluemix:public:quantum-computing:us-east:a/def:2222::"
CRN_STALE = "crn:v1:bluemix:public:quantum-computing:us-east:a/old:3333::"


class FakeSession:
    _timeout = None


class FakeClient:
    def __init__(self, crn, backends):
        self._crn = crn
        self._backends = backends
        self._session = FakeSession()

    def list_backends(self):
        return [{"name": name} for name in self._backends]


class FakeService:
    instances_map = {}
    inaccessible = set()

    def __init__(self, channel=None, token=None, instance=None):
        if instance in self.inaccessible or (instance not in (None, "auto") and instance not in self.instances_map):
            raise IBMInputValueError(
                "The given API token is associated with an account that does not have access to the instance."
            )
        if instance in (None, "auto"):
            if not self.instances_map:
                raise IBMInputValueError("No instances available for this account.")
            self._crn = next(iter(self.instances_map))
        else:
            self._crn = instance

    def _get_api_client(self, crn):
        if crn not in self.instances_map:
            raise IBMInputValueError("No access to instance.")
        return FakeClient(crn, self.instances_map[crn])

    def active_instance(self):
        return self._crn

    def instances(self):
        return [
            {"crn": crn, "name": "instance-%d" % index, "plan": "open"}
            for index, crn in enumerate(self.instances_map)
        ]


class TestIBMInstanceResolution(unittest.TestCase):
    def setUp(self):
        FakeService.instances_map = {}
        FakeService.inaccessible = set()
        self.env = {"IBM_QUANTUM_API_KEY": "test-key", "IBM_QUANTUM_INSTANCE": ""}
        self.patcher = patch("ibm_collect.QiskitRuntimeService", FakeService)
        self.patcher.start()
        self.env_patcher = patch.dict(os.environ, self.env)
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.patcher.stop()

    def use_instance(self, crn):
        os.environ["IBM_QUANTUM_INSTANCE"] = crn

    def test_explicit_instance_is_used_when_accessible(self):
        FakeService.instances_map = {CRN_GOOD: list(ibm_collect.BACKENDS)}
        self.use_instance(CRN_GOOD)
        client = ibm_collect.connect()
        self.assertEqual(client._crn, CRN_GOOD)

    def test_stale_instance_secret_falls_back_to_accessible_instance(self):
        FakeService.instances_map = {CRN_OTHER: list(ibm_collect.BACKENDS)}
        FakeService.inaccessible = {CRN_STALE}
        self.use_instance(CRN_STALE)
        client = ibm_collect.connect()
        self.assertEqual(client._crn, CRN_OTHER)

    def test_instance_without_all_backends_is_skipped(self):
        FakeService.instances_map = {
            CRN_GOOD: list(ibm_collect.BACKENDS[:-1]),
            CRN_OTHER: list(ibm_collect.BACKENDS),
        }
        self.use_instance(CRN_GOOD)
        client = ibm_collect.connect()
        self.assertEqual(client._crn, CRN_OTHER)

    def test_no_instance_exposes_all_backends(self):
        FakeService.instances_map = {CRN_GOOD: list(ibm_collect.BACKENDS[:-1])}
        self.use_instance(CRN_GOOD)
        with self.assertRaises(RuntimeError) as caught:
            ibm_collect.connect()
        self.assertIn(ibm_collect.BACKENDS[-1], str(caught.exception))

    def test_missing_api_key_raises_clear_error(self):
        os.environ["IBM_QUANTUM_API_KEY"] = ""
        with self.assertRaises(RuntimeError) as caught:
            ibm_collect.connect()
        self.assertIn("IBM_QUANTUM_API_KEY", str(caught.exception))

    def test_bad_crn_format_raises_clear_error(self):
        self.use_instance("not-a-crn")
        with self.assertRaises(RuntimeError) as caught:
            ibm_collect.connect()
        self.assertIn("crn:v1:", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
