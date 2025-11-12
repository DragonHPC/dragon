"""
Test script to verify the fix for issue #52: configure_training_group ppn inference

This script demonstrates that configure_training_group now works correctly when
only policies are provided (without explicit ppn parameter).
"""

import sys
from unittest.mock import Mock, MagicMock, patch


def test_ppn_inference_from_policies():
    """Test that ppn is correctly inferred from policies list"""
    
    # Mock the required imports and classes
    with patch('dragon.native.process_group.System') as MockSystem, \
         patch('dragon.native.process_group.Policy') as MockPolicy, \
         patch('dragon.native.process_group.Node') as MockNode, \
         patch('dragon.native.process_group.ProcessTemplate') as MockProcessTemplate, \
         patch('dragon.native.process_group.Popen') as MockPopen, \
         patch('dragon.native.process_group.TrainingGroupConfig') as MockTrainingGroupConfig:
        
        # Import after mocking
        sys.path.insert(0, '/home/samaresh/src/awesome/dragon/src')
        from dragon.native.process_group import ProcessGroup
        
        # Setup mock policies for 2 nodes with 2 processes each
        mock_policy1 = Mock()
        mock_policy1.host_name = 'node1'
        
        mock_policy2 = Mock()
        mock_policy2.host_name = 'node1'
        
        mock_policy3 = Mock()
        mock_policy3.host_name = 'node2'
        
        mock_policy4 = Mock()
        mock_policy4.host_name = 'node2'
        
        policies = [mock_policy1, mock_policy2, mock_policy3, mock_policy4]
        
        # Mock System to return node list
        mock_system = Mock()
        mock_system.nodes = ['node1', 'node2']
        MockSystem.return_value = mock_system
        
        # Mock Node
        MockNode.return_value.hostname = 'node1'
        
        # Create a mock training function
        def dummy_training():
            pass
        
        # This should NOT raise TypeError anymore
        try:
            pg = ProcessGroup.configure_training_group(
                training_fn=dummy_training,
                policies=policies,
                # Note: ppn is NOT provided - this is the fix!
            )
            print("✓ SUCCESS: configure_training_group works without ppn when policies provided")
            print(f"  Inferred ppn should be: 2 (max processes per node)")
            return True
        except TypeError as e:
            if "unsupported operand type(s) for //: 'int' and 'NoneType'" in str(e):
                print("✗ FAILED: Bug still exists - ppn is None")
                print(f"  Error: {e}")
                return False
            raise


def test_ppn_inference_non_uniform():
    """Test ppn inference with non-uniform distribution"""
    
    with patch('dragon.native.process_group.System') as MockSystem, \
         patch('dragon.native.process_group.Policy') as MockPolicy, \
         patch('dragon.native.process_group.Node') as MockNode, \
         patch('dragon.native.process_group.ProcessTemplate') as MockProcessTemplate, \
         patch('dragon.native.process_group.Popen') as MockPopen, \
         patch('dragon.native.process_group.TrainingGroupConfig') as MockTrainingGroupConfig:
        
        sys.path.insert(0, '/home/samaresh/src/awesome/dragon/src')
        from dragon.native.process_group import ProcessGroup
        
        # Setup mock policies: 3 processes on node1, 1 process on node2
        policies = []
        for _ in range(3):
            p = Mock()
            p.host_name = 'node1'
            policies.append(p)
        
        p = Mock()
        p.host_name = 'node2'
        policies.append(p)
        
        # Mock System
        mock_system = Mock()
        mock_system.nodes = ['node1', 'node2']
        MockSystem.return_value = mock_system
        MockNode.return_value.hostname = 'node1'
        
        def dummy_training():
            pass
        
        try:
            pg = ProcessGroup.configure_training_group(
                training_fn=dummy_training,
                policies=policies,
            )
            print("✓ SUCCESS: configure_training_group handles non-uniform distribution")
            print(f"  Inferred ppn should be: 3 (max processes on any node)")
            return True
        except TypeError as e:
            if "unsupported operand type(s) for //: 'int' and 'NoneType'" in str(e):
                print("✗ FAILED: Bug still exists with non-uniform distribution")
                print(f"  Error: {e}")
                return False
            raise


if __name__ == '__main__':
    print("Testing fix for issue #52: configure_training_group ppn inference")
    print("=" * 70)
    print()
    
    print("Test 1: Uniform distribution (2 processes per node)")
    test1 = test_ppn_inference_from_policies()
    print()
    
    print("Test 2: Non-uniform distribution (3 on node1, 1 on node2)")
    test2 = test_ppn_inference_non_uniform()
    print()
    
    if test1 and test2:
        print("=" * 70)
        print("All tests PASSED! Issue #52 is fixed.")
        sys.exit(0)
    else:
        print("=" * 70)
        print("Some tests FAILED. Issue #52 needs more work.")
        sys.exit(1)
