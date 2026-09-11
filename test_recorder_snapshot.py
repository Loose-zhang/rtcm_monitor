import tempfile
import unittest

import recorder
import app


class _CompleteMsm:
    DF393 = 0
    DF004 = 123456


class RecorderSnapshotTest(unittest.TestCase):
    def test_candidate_and_sealed_snapshot_are_reported(self):
        snapshots = []
        satellites = [{
            "sat": "C01", "sys": "C", "prn": 1,
            "az": 120.5, "el": 45.2, "cn0": 47.0,
        }, {
            "sat": "C02", "sys": "C", "prn": 2,
            "az": None, "el": None, "cn0": 40.0,
        }]
        with tempfile.TemporaryDirectory() as folder:
            rec = recorder.Recorder(
                folder, "5050001", "rover", "测站", "5050001",
                sat_snapshot_fn=lambda: satellites,
                snapshot_saved_fn=snapshots.append)
            rec.feed(b"\xd3\x00\x00", _CompleteMsm(), now=1788800455)

            self.assertEqual(1, len(snapshots))
            self.assertFalse(snapshots[0]["sealed"])
            self.assertEqual("rover", snapshots[0]["role"])
            self.assertEqual(1788800400, snapshots[0]["periodStart"])
            self.assertEqual(["C01"], [p["sat"] for p in snapshots[0]["satellites"]])

            rec.close()
            self.assertEqual(2, len(snapshots))
            self.assertTrue(snapshots[1]["sealed"])

    def test_security_monitor_binding_requires_matching_sid(self):
        client = app.app.test_client()
        bad = client.post("/api/security-monitor/bind", json={
            "sid": "5050001", "token": "token-1",
            "roverId": "5050999", "baseId": "5050002",
        })
        self.assertEqual(400, bad.status_code)

        good = client.post("/api/security-monitor/bind", json={
            "sid": "5050001", "token": "token-1",
            "roverId": "5050001", "baseId": "5050002",
        })
        self.assertEqual(200, good.status_code)
        state = app.MANAGER.get("5050001")
        self.assertEqual("token-1", state.security_monitor_token)


if __name__ == "__main__":
    unittest.main()
