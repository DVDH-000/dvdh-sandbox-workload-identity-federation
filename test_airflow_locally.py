#!/usr/bin/env python3
"""
Test Airflow WIF DAG locally
This runs the DAG tasks directly in the local Airflow environment
"""

import os
import sys
import subprocess
import time
from datetime import datetime

def set_airflow_env():
    """Set up Airflow environment variables"""
    airflow_home = os.path.join(os.getcwd(), "airflow_local")
    os.environ["AIRFLOW_HOME"] = airflow_home
    print(f"🏠 AIRFLOW_HOME set to: {airflow_home}")
    return airflow_home

def test_dag_parsing():
    """Test if the DAG parses correctly"""
    print("\n📋 TESTING DAG PARSING")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "airflow", "dags", "list"
        ], capture_output=True, text=True, timeout=30)
        
        if "working_wif_gcs_to_s3" in result.stdout:
            print("✅ DAG 'working_wif_gcs_to_s3' found and parsed successfully")
            return True
        else:
            print("❌ DAG not found in list")
            print(f"Output: {result.stdout}")
            print(f"Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ DAG parsing timed out")
        return False
    except Exception as e:
        print(f"❌ DAG parsing failed: {e}")
        return False

def show_dag_info():
    """Show DAG information"""
    print("\n📊 DAG INFORMATION")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "airflow", "dags", "show", "working_wif_gcs_to_s3"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ DAG structure:")
            print(result.stdout)
        else:
            print(f"⚠️  Could not show DAG structure: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error showing DAG info: {e}")

def test_task_directly(task_id):
    """Test a specific task directly"""
    print(f"\n🔧 TESTING TASK: {task_id}")
    print("=" * 50)
    
    try:
        # Use today's date
        execution_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"🎯 Running task: {task_id}")
        print(f"📅 Execution date: {execution_date}")
        
        result = subprocess.run([
            sys.executable, "-m", "airflow", "tasks", "test",
            "working_wif_gcs_to_s3", task_id, execution_date
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        print(f"📤 Return code: {result.returncode}")
        
        if result.returncode == 0:
            print(f"✅ Task '{task_id}' completed successfully!")
            if result.stdout:
                print("📋 Output:")
                # Show last 20 lines of output
                lines = result.stdout.split('\n')
                for line in lines[-20:]:
                    if line.strip():
                        print(f"   {line}")
            return True
        else:
            print(f"❌ Task '{task_id}' failed!")
            if result.stderr:
                print("📋 Error output:")
                # Show last 10 lines of error
                lines = result.stderr.split('\n')
                for line in lines[-10:]:
                    if line.strip():
                        print(f"   {line}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"❌ Task '{task_id}' timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ Error running task '{task_id}': {e}")
        return False

def run_full_dag_test():
    """Run all tasks in the DAG sequentially"""
    print("\n🚀 RUNNING FULL DAG TEST")
    print("=" * 60)
    
    # Tasks in order of execution
    tasks = [
        "test_aws_connection",
        "simple_file_transfer", 
        "bulk_gcs_to_s3_transfer"
    ]
    
    results = []
    
    for task in tasks:
        success = test_task_directly(task)
        results.append((task, success))
        
        if not success:
            print(f"❌ Stopping due to task failure: {task}")
            break
        
        # Small delay between tasks
        time.sleep(2)
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 AIRFLOW DAG TEST RESULTS")
    print(f"{'='*60}")
    
    for task, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {task}")
    
    passed_tasks = sum(1 for _, success in results if success)
    total_tasks = len(results)
    
    print(f"\n📈 Results: {passed_tasks}/{total_tasks} tasks passed")
    
    if passed_tasks == total_tasks:
        print("🎉 ALL AIRFLOW TASKS PASSED!")
        print("✅ WIF implementation works perfectly in Airflow")
        return True
    else:
        print("⚠️  SOME AIRFLOW TASKS FAILED!")
        print("🔧 Check the error messages above")
        return False

def create_user_if_needed():
    """Create admin user if it doesn't exist"""
    print("\n👤 ENSURING ADMIN USER EXISTS")
    print("=" * 50)
    
    try:
        # Try to create user (will fail if exists, which is fine)
        result = subprocess.run([
            sys.executable, "-m", "airflow", "users", "create",
            "--username", "admin",
            "--firstname", "Admin", 
            "--lastname", "User",
            "--role", "Admin",
            "--email", "admin@example.com",
            "--password", "admin"
        ], capture_output=True, text=True)
        
        if "already exists" in result.stderr:
            print("✅ Admin user already exists")
        elif result.returncode == 0:
            print("✅ Admin user created")
        else:
            print(f"⚠️  User creation: {result.stderr}")
        
        return True
        
    except Exception as e:
        print(f"❌ User creation error: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 LOCAL AIRFLOW WIF DAG TEST")
    print("=" * 80)
    print("🎯 Testing the working WIF DAG in local Airflow environment")
    
    # Set up environment
    airflow_home = set_airflow_env()
    
    # Ensure user exists
    create_user_if_needed()
    
    # Test DAG parsing
    if not test_dag_parsing():
        print("❌ DAG parsing failed - cannot continue")
        return False
    
    # Show DAG info
    show_dag_info()
    
    # Run full test
    success = run_full_dag_test()
    
    # Final summary
    print(f"\n{'='*80}")
    print("🎯 FINAL RESULTS")
    print(f"{'='*80}")
    
    if success:
        print("🎉 SUCCESS: WIF DAG works perfectly in Airflow!")
        print("✅ All tasks executed successfully")
        print("🚀 Ready for Cloud Composer deployment")
        
        print(f"\n📋 To view in Airflow UI:")
        print(f"1. Open terminal and run: export AIRFLOW_HOME={airflow_home}")
        print("2. Run: airflow webserver --port 8080")
        print("3. Open: http://localhost:8080")
        print("4. Login: admin / admin")
        print("5. Find DAG: working_wif_gcs_to_s3")
        
    else:
        print("❌ FAILURE: Some tasks failed")
        print("🔧 Review the error messages above")
        print("💡 The local test results help identify any remaining issues")
    
    return success

if __name__ == "__main__":
    main()