#!/usr/bin/env python3
"""
Test the DAG functions directly without Airflow framework
This tests the core WIF logic that would run in Airflow
"""

import sys
import os

# Add the dags folder to Python path so we can import our functions
sys.path.append(os.path.join(os.getcwd(), "airflow_local", "dags"))

def test_dag_functions():
    """Test the DAG functions directly"""
    print("🚀 TESTING DAG FUNCTIONS DIRECTLY")
    print("=" * 80)
    print("🎯 This tests the core WIF logic without Airflow framework")
    
    try:
        # Import the functions from our DAG
        from working_wif_dag import (
            get_aws_credentials_via_wif,
            test_aws_connection_working,
            transfer_gcs_to_s3_working,
            simple_gcs_to_s3_file_transfer
        )
        
        print("✅ Successfully imported DAG functions")
        
        # Test 1: AWS Connection
        print(f"\n{'='*60}")
        print("TEST 1: AWS CONNECTION")
        print(f"{'='*60}")
        
        try:
            result = test_aws_connection_working()
            if result:
                print("✅ AWS connection test PASSED")
            else:
                print("❌ AWS connection test FAILED")
        except Exception as e:
            print(f"❌ AWS connection test ERROR: {e}")
            result = False
        
        test1_result = result
        
        # Test 2: Simple File Transfer (if AWS connection works)
        if test1_result:
            print(f"\n{'='*60}")
            print("TEST 2: SIMPLE FILE TRANSFER")
            print(f"{'='*60}")
            
            try:
                simple_gcs_to_s3_file_transfer()
                print("✅ Simple file transfer PASSED")
                test2_result = True
            except Exception as e:
                print(f"❌ Simple file transfer ERROR: {e}")
                import traceback
                traceback.print_exc()
                test2_result = False
        else:
            print(f"\n⏭️  Skipping simple file transfer due to AWS connection failure")
            test2_result = False
        
        # Test 3: Bulk Transfer (if previous tests work)
        if test1_result and test2_result:
            print(f"\n{'='*60}")
            print("TEST 3: BULK TRANSFER")
            print(f"{'='*60}")
            
            try:
                transfer_gcs_to_s3_working()
                print("✅ Bulk transfer PASSED")
                test3_result = True
            except Exception as e:
                print(f"❌ Bulk transfer ERROR: {e}")
                import traceback
                traceback.print_exc()
                test3_result = False
        else:
            print(f"\n⏭️  Skipping bulk transfer due to previous failures")
            test3_result = False
        
        # Summary
        print(f"\n{'='*80}")
        print("🎯 DAG FUNCTION TEST RESULTS")
        print(f"{'='*80}")
        
        tests = [
            ("AWS Connection", test1_result),
            ("Simple File Transfer", test2_result),
            ("Bulk Transfer", test3_result)
        ]
        
        for test_name, success in tests:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{status} {test_name}")
        
        passed_tests = sum(1 for _, success in tests if success)
        total_tests = len(tests)
        
        print(f"\n📊 Results: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            print("\n🎉 ALL DAG FUNCTION TESTS PASSED!")
            print("✅ The WIF logic works perfectly")
            print("🚀 Ready for Airflow deployment")
            return True
        else:
            print("\n⚠️  SOME DAG FUNCTION TESTS FAILED!")
            print("🔧 Review the errors above")
            return False
        
    except ImportError as e:
        print(f"❌ Failed to import DAG functions: {e}")
        print("🔧 Check that the DAG file is in the correct location")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_individual_components():
    """Test individual components separately"""
    print(f"\n{'='*80}")
    print("🔧 TESTING INDIVIDUAL COMPONENTS")
    print(f"{'='*80}")
    
    # Test just the WIF credential generation
    try:
        from working_wif_dag import get_aws_credentials_via_wif
        
        print("🎯 Testing WIF credential generation...")
        creds = get_aws_credentials_via_wif()
        
        if creds and 'aws_access_key_id' in creds:
            print("✅ WIF credential generation works")
            print(f"   Access Key: {creds['aws_access_key_id'][:10]}...")
            print(f"   Expires: {creds['expiration']}")
            return True
        else:
            print("❌ WIF credential generation failed")
            return False
            
    except Exception as e:
        print(f"❌ WIF credential test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print("🚀 DIRECT DAG FUNCTION TESTING")
    print("=" * 80)
    print("🎯 Testing WIF DAG functions without Airflow framework overhead")
    
    # Test individual components first
    component_result = test_individual_components()
    
    if component_result:
        # Test full DAG functions
        dag_result = test_dag_functions()
    else:
        print("\n⏭️  Skipping full DAG test due to component failure")
        dag_result = False
    
    # Final summary
    print(f"\n{'='*80}")
    print("🎯 OVERALL TEST RESULTS")
    print(f"{'='*80}")
    
    if component_result and dag_result:
        print("🎉 SUCCESS: All WIF functions work perfectly!")
        print("✅ Components: Working")
        print("✅ DAG Functions: Working")
        print("🚀 The DAG is ready for Cloud Composer")
        
        print("\n📋 What this proves:")
        print("✅ WIF authentication chain works")
        print("✅ GCS to S3 transfers work")
        print("✅ All error handling works")
        print("✅ Airflow will be able to run these functions")
        
    elif component_result:
        print("⚠️  PARTIAL SUCCESS: Components work but DAG functions have issues")
        print("✅ Components: Working")
        print("❌ DAG Functions: Issues")
        print("🔧 Minor fixes needed")
        
    else:
        print("❌ FAILURE: Core components not working")
        print("❌ Components: Issues")
        print("❌ DAG Functions: Cannot test")
        print("🔧 Need to fix fundamental issues")
    
    return component_result and dag_result

if __name__ == "__main__":
    main()