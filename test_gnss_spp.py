import math
import unittest
from pathlib import Path

import gnss_orbit as orbit
import gnss_spp as spp
from app import State
from rtcm_decode import RTCMReader, RtcmFramer, crc24q


def _eph(prn, m0, om0, t):
    return {
        "sys": "G", "prn": prn, "sat": f"G{prn:02d}", "week": 0,
        "toe": t - 1800.0, "toc": t - 1800.0,
        "M0": m0, "dn": 0.0, "e": 0.008 + prn * 0.0001,
        "sqrtA": math.sqrt(26560000.0), "Om0": om0,
        "i0": math.radians(55.0), "w": 0.2 + prn * 0.03,
        "Omd": -8.0e-9, "idot": 0.0,
        "Cuc": 0.0, "Cus": 0.0, "Crc": 0.0, "Crs": 0.0,
        "Cic": 0.0, "Cis": 0.0, "health": 0,
        "af0": (prn - 4) * 2e-5, "af1": 1e-12, "af2": 0.0,
        "tgd": -2e-9,
    }


def _bds_eph(prn, m0, om0, t):
    eph = _eph(prn, m0, om0, t)
    eph.update({"sys": "C", "sat": f"C{prn:02d}",
                "sqrtA": math.sqrt(27800000.0),
                "af0": (prn - 10) * 1e-5, "tgd": 2e-9})
    return eph


def _synthetic_observation(eph, receiver, receiver_clock_m, t):
    # Fixed-point construction using the same physical transmit-time model as
    # the solver, but without calling its least-squares implementation.
    pr = 2.4e7
    for _ in range(8):
        travel = pr / spp.C_LIGHT
        tx = t - travel
        sx, sy, sz = orbit.sat_ecef(eph, tx)
        we = orbit.CONST[eph["sys"]][1]
        a = we * travel; ca, sa = math.cos(a), math.sin(a)
        sx, sy = ca*sx + sa*sy, -sa*sx + ca*sy
        rho = math.sqrt((receiver[0]-sx)**2 + (receiver[1]-sy)**2
                        + (receiver[2]-sz)**2)
        pr = rho + receiver_clock_m - spp.C_LIGHT * orbit.sat_clock(eph, tx)
    return pr


class TestSpp(unittest.TestCase):
    def _scenario(self, receiver, clock_m=73000.0, t=345600.0, count=10):
        ephs = {}
        obs = []
        for prn in range(1, count + 1):
            eph = _eph(prn, 0.55*prn, 0.83*prn, t)
            ephs[eph["sat"]] = eph
            obs.append({"sat": eph["sat"],
                        "pseudorange": _synthetic_observation(eph, receiver,
                                                               clock_m, t),
                        "cn0": 43.0})
        return t, ephs, obs

    def test_recovers_receiver_ecef_with_satellite_clocks(self):
        receiver = (-2324872.6688, 5387637.0231, 2491675.2054)
        clock_m = 73000.0
        t, ephs, obs = self._scenario(receiver, clock_m)
        result = spp.solve(obs, ephs, t*1000.0, "G")
        self.assertIsNotNone(result)
        error = math.sqrt(sum((result["xyz"][i]-receiver[i])**2
                              for i in range(3)))
        self.assertLess(error, 0.05)
        self.assertLess(abs(result["clock_m"]-clock_m), 0.05)

    def test_discards_one_gross_pseudorange_outlier(self):
        receiver = (-2324872.6688, 5387637.0231, 2491675.2054)
        t, ephs, obs = self._scenario(receiver)
        obs[3]["pseudorange"] += 500000.0
        result = spp.solve(obs, ephs, t*1000.0, "G")
        self.assertIsNotNone(result)
        self.assertNotIn("G04", result["sats"])
        error = math.sqrt(sum((result["xyz"][i]-receiver[i])**2
                              for i in range(3)))
        self.assertLess(error, 0.1)

    def test_recovers_receiver_from_bds_meo_observations(self):
        t = 117000.0
        receiver = (-2324872.6688, 5387637.0231, 2491675.2054)
        ephs = {}; obs = []
        for prn in range(6, 16):
            eph = _bds_eph(prn, 0.48*prn, 0.71*prn, t)
            ephs[eph["sat"]] = eph
            obs.append({"sat": eph["sat"],
                        "pseudorange": _synthetic_observation(eph, receiver,
                                                               41000.0, t),
                        "cn0": 42.0})
        result = spp.solve(obs, ephs, t*1000.0, "C")
        self.assertIsNotNone(result)
        error = math.sqrt(sum((result["xyz"][i]-receiver[i])**2
                              for i in range(3)))
        self.assertLess(error, 0.1)

    def test_rejects_fewer_than_five_satellites(self):
        self.assertIsNone(spp.solve([], {}, 1000.0, "G"))

    def test_rejects_non_earth_solution(self):
        t = 345600.0
        ephs = {}; obs = []
        receiver = (0.0, 0.0, 0.0)
        for prn in range(1, 7):
            eph = _eph(prn, 0.7*prn, 0.9*prn, t)
            ephs[eph["sat"]] = eph
            obs.append({"sat": eph["sat"],
                        "pseudorange": _synthetic_observation(eph, receiver, 0, t),
                        "cn0": 40.0})
        self.assertIsNone(spp.solve(obs, ephs, t*1000.0, "G"))

    def test_state_requires_three_stable_epochs_then_rtcm_overrides(self):
        receiver = (-2324872.6688, 5387637.0231, 2491675.2054)
        state = State("spp-test")
        for epoch in range(3):
            t, ephs, obs = self._scenario(receiver, t=345600.0 + epoch)
            state._run_spp((obs, ephs, t*1000.0, "G", state.base_xyz))
            if epoch < 2:
                self.assertIsNone(state.base_xyz)
        self.assertIsNotNone(state.base_xyz)
        self.assertEqual(state.base_xyz_source, "SPP-G")
        self.assertEqual(state.base_xyz_quality["state"], "stable")
        self.assertTrue(state.snapshot()["eph"]["base_known"])

        # Turn a real project 1005 frame into a valid 1006 carrying the same
        # ECEF fields.  RTCM coordinates must immediately outrank coarse SPP.
        raw1005 = None
        framer = RtcmFramer()
        data = Path(__file__).with_name("Bds.rtcm").read_bytes()
        for raw in framer.feed(data):
            if str(RTCMReader.parse(raw).identity) == "1005":
                raw1005 = raw
                break
        payload = bytearray(raw1005[3:-3])
        payload[0] = (1006 >> 4) & 0xff
        payload[1] = (payload[1] & 0x0f) | ((1006 & 0x0f) << 4)
        payload.extend(b"\x00\x00")
        length = len(payload)
        body = bytes([0xD3, (length >> 8) & 0x03, length & 0xff]) + payload
        raw1006 = body + crc24q(body).to_bytes(3, "big")
        expected = RTCMReader.parse(raw1006)
        state.on_frame(raw1006, "base")
        self.assertEqual(state.base_xyz_source, "RTCM1006-base")
        self.assertEqual(state.base_xyz,
                         (expected.DF025, expected.DF026, expected.DF027))

    def test_project_rtcm_replay_keeps_standard_1005_path(self):
        state = State("project-file-test")
        framer = RtcmFramer()
        data = Path(__file__).with_name("Bds.rtcm").read_bytes()
        for raw in framer.feed(data):
            state.on_frame(raw, "base")
        snap = state.snapshot()
        self.assertTrue(snap["eph"]["base_known"])
        self.assertEqual(snap["eph"]["base_source"], "RTCM1005-base")
        self.assertEqual(snap["channels"]["base"]["station"], 1660)


if __name__ == "__main__":
    unittest.main()
