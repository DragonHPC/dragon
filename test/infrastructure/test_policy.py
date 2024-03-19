import unittest
from dataclasses import asdict, dataclass, field
from dragon.infrastructure.policy import Policy
import threading


class PolicyTester(unittest.TestCase):
    def testit():
        with Policy(distribution=Policy.Distribution.BLOCK):
            # In infrastructure.process, channel, and pool you can use the code that I provided from
            # infrastructure.process to pass the top of the stack in the messages sent to global services.
            # For instance, you would access the top like I have below if the policy is not None that was
            # passed in (see process.py in the branch I pushed). Don't bother changing api_setup.py, the
            # better spot to have the policy stack is in policy.py.
            self.assertInstance(Policy.global_policy(), dragon.infrastructure.policy.Policy)

        self.assertInstance(Policy.global_policy(), dragon.infrastructure.policy.Policy)

    def test_policy(self):
        with Policy(distribution=Policy.Distribution.DEFAULT):
            # In infrastructure.process, channel, and pool you can use the code that I provided from
            # infrastructure.process to pass the top of the stack in the messages sent to global services.
            # For instance, you would access the top like I have below if the policy is not None that was
            # passed in (see process.py in the branch I pushed). Don't bother changing api_setup.py, the
            # better spot to have the policy stack is in policy.py.
            self.assertInstance(Policy.global_policy(), dragon.infrastructure.policy.Policy)
            t = threading.Thread(target=testit, args=())
            t.start()
            t.join()
            self.assertInstance(Policy.global_policy(), dragon.infrastructure.policy.Policy)

        self.assertInstance(Policy.global_policy(), dragon.infrastructure.policy.Policy)

if __name__ == "__main__":
    unittest.main()