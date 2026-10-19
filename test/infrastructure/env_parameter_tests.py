import copy
import os
import unittest
import dragon.infrastructure.parameters as dip


class LaunchParameterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.all_names = dip.LaunchParameters.all_parm_names()

        # need this to keep this test case from poisoning
        # other tests that might be run together with other ones
        # in the same process
        self.old_env = dict(os.environ)
        self.old_this_proc = copy.copy(dip.this_process)

    def tearDown(self) -> None:
        for k, v in self.old_env.items():
            os.environ[k] = v

        for k, v in self.old_this_proc.env().items():
            os.environ[k] = v

        dip.this_process = self.old_this_proc

    def test_smoke(self):
        mlp = dip.LaunchParameters.from_env()
        mlp2 = dip.LaunchParameters.from_env()

        for name in self.all_names:
            self.assertEqual(getattr(mlp, name), getattr(mlp2, name), f"looking for {name}")

    def test_from_env(self):
        key_to_test = "INF_SEG_SZ"
        old_val = None
        ev_name = dip.LaunchParameters._ev_name_from_pt(key_to_test)

        if ev_name in os.environ:
            old_val = os.environ[ev_name]

        os.environ[ev_name] = str(17)

        mlp = dip.LaunchParameters.from_env()

        self.assertEqual(mlp.inf_seg_sz, 17)

        if old_val is not None:
            os.environ[ev_name] = old_val

    def test_to_env(self):
        key_to_test = "INF_SEG_SZ"
        ev_name = dip.LaunchParameters._ev_name_from_pt(key_to_test)
        mlp = dip.LaunchParameters.from_env()
        mlp.inf_seg_sz = 1001
        the_env = mlp.env()

        self.assertEqual(mlp.inf_seg_sz, int(the_env[ev_name]))


if __name__ == "__main__":
    unittest.main()
