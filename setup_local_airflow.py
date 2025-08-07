#!/usr/bin/env python3
"""
Setup local Airflow environment for testing WIF DAG
This will install and configure Airflow locally
"""

import subprocess
import sys
import os
from pathlib import Path

def install_airflow():
    """Install Apache Airflow locally"""
    print("🚀 SETTING UP LOCAL AIRFLOW")
    print("=" * 60)
    
    # Airflow version and constraints
    airflow_version = "2.7.3"
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    constraint_url = f"https://raw.githubusercontent.com/apache/airflow/constraints-{airflow_version}/constraints-{python_version}.txt"
    
    print(f"📋 Installing Airflow {airflow_version} for Python {python_version}")
    
    # Install Airflow with required providers
    packages = [
        f"apache-airflow=={airflow_version}",
        "apache-airflow-providers-amazon",
        "apache-airflow-providers-google"
    ]
    
    for package in packages:
        print(f"📦 Installing {package}...")
        try:
            subprocess.run([
                sys.executable, "-m", "pip", "install", 
                package, "--constraint", constraint_url
            ], check=True, capture_output=True)
            print(f"✅ {package} installed")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {package}: {e}")
            return False
    
    return True

def setup_airflow_home():
    """Set up Airflow home directory"""
    print("\n🏠 SETTING UP AIRFLOW HOME")
    print("=" * 60)
    
    # Set AIRFLOW_HOME to current directory
    airflow_home = os.path.join(os.getcwd(), "airflow_local")
    os.environ["AIRFLOW_HOME"] = airflow_home
    
    print(f"📁 AIRFLOW_HOME: {airflow_home}")
    
    # Create directories
    Path(airflow_home).mkdir(exist_ok=True)
    Path(os.path.join(airflow_home, "dags")).mkdir(exist_ok=True)
    Path(os.path.join(airflow_home, "logs")).mkdir(exist_ok=True)
    Path(os.path.join(airflow_home, "plugins")).mkdir(exist_ok=True)
    
    return airflow_home

def create_airflow_config(airflow_home):
    """Create Airflow configuration"""
    print("\n⚙️  CREATING AIRFLOW CONFIG")
    print("=" * 60)
    
    config_content = f"""
[core]
dags_folder = {airflow_home}/dags
base_log_folder = {airflow_home}/logs
plugins_folder = {airflow_home}/plugins
executor = LocalExecutor
sql_alchemy_conn = sqlite:///{airflow_home}/airflow.db
load_examples = False
dagbag_import_timeout = 30

[scheduler]
dag_dir_list_interval = 10
catchup_by_default = False

[webserver]
web_server_port = 8080
web_server_host = 0.0.0.0
secret_key = local_test_secret_key

[logging]
logging_level = INFO
"""
    
    config_path = os.path.join(airflow_home, "airflow.cfg")
    with open(config_path, "w") as f:
        f.write(config_content)
    
    print(f"✅ Config created: {config_path}")
    return config_path

def initialize_airflow_db(airflow_home):
    """Initialize Airflow database"""
    print("\n🗄️  INITIALIZING AIRFLOW DATABASE")
    print("=" * 60)
    
    try:
        # Set environment variable
        env = os.environ.copy()
        env["AIRFLOW_HOME"] = airflow_home
        
        # Initialize database
        print("📊 Running airflow db init...")
        result = subprocess.run([
            sys.executable, "-m", "airflow", "db", "init"
        ], env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Database initialized successfully")
            return True
        else:
            print(f"❌ Database initialization failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        return False

def create_admin_user(airflow_home):
    """Create admin user for Airflow webserver"""
    print("\n👤 CREATING ADMIN USER")
    print("=" * 60)
    
    try:
        env = os.environ.copy()
        env["AIRFLOW_HOME"] = airflow_home
        
        # Create admin user
        print("👤 Creating admin user...")
        result = subprocess.run([
            sys.executable, "-m", "airflow", "users", "create",
            "--username", "admin",
            "--firstname", "Admin",
            "--lastname", "User", 
            "--role", "Admin",
            "--email", "admin@example.com",
            "--password", "admin"
        ], env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Admin user created")
            print("   Username: admin")
            print("   Password: admin")
            return True
        else:
            print(f"⚠️  User creation result: {result.stderr}")
            # User might already exist, that's ok
            return True
            
    except Exception as e:
        print(f"❌ User creation error: {e}")
        return False

def copy_dag_to_airflow(airflow_home):
    """Copy our working WIF DAG to Airflow DAGs folder"""
    print("\n📁 COPYING WIF DAG TO AIRFLOW")
    print("=" * 60)
    
    try:
        import shutil
        
        # Source DAG file
        source_dag = "working_wif_dag.py"
        
        # Destination
        dags_folder = os.path.join(airflow_home, "dags")
        dest_dag = os.path.join(dags_folder, source_dag)
        
        # Copy DAG
        shutil.copy2(source_dag, dest_dag)
        print(f"✅ Copied {source_dag} to {dest_dag}")
        
        # Also copy the custom hook if it exists
        if os.path.exists("custom_wif_hook.py"):
            dest_hook = os.path.join(dags_folder, "custom_wif_hook.py")
            shutil.copy2("custom_wif_hook.py", dest_hook)
            print(f"✅ Copied custom_wif_hook.py to {dest_hook}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to copy DAG: {e}")
        return False

def create_airflow_connection(airflow_home):
    """Create the AWS connection in Airflow"""
    print("\n🔗 CREATING AIRFLOW CONNECTION")
    print("=" * 60)
    
    try:
        env = os.environ.copy()
        env["AIRFLOW_HOME"] = airflow_home
        
        # Connection configuration (simplified since our DAG doesn't use Airflow connections)
        connection_config = {
            "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
        }
        
        # Add connection
        print("🔗 Adding AWS connection...")
        result = subprocess.run([
            sys.executable, "-m", "airflow", "connections", "add",
            "gcp_to_aws_s3_sandbox_davy",
            "--conn-type", "aws",
            "--conn-extra", f"'{connection_config}'"
        ], env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Connection created successfully")
        else:
            print(f"⚠️  Connection creation: {result.stderr}")
            # Connection might already exist
        
        return True
        
    except Exception as e:
        print(f"❌ Connection creation error: {e}")
        return False

def show_startup_commands(airflow_home):
    """Show commands to start Airflow"""
    print("\n🚀 AIRFLOW STARTUP COMMANDS")
    print("=" * 60)
    
    print("To start Airflow locally, run these commands in separate terminals:")
    print()
    print("Terminal 1 (Scheduler):")
    print(f"export AIRFLOW_HOME={airflow_home}")
    print("airflow scheduler")
    print()
    print("Terminal 2 (Webserver):")
    print(f"export AIRFLOW_HOME={airflow_home}")
    print("airflow webserver --port 8080")
    print()
    print("Then open: http://localhost:8080")
    print("Login: admin / admin")
    print()
    print("Look for the DAG: 'working_wif_gcs_to_s3'")

def main():
    """Main setup function"""
    print("🚀 LOCAL AIRFLOW SETUP FOR WIF TESTING")
    print("=" * 80)
    
    # Step 1: Install Airflow
    if not install_airflow():
        print("❌ Airflow installation failed")
        return False
    
    # Step 2: Setup Airflow home
    airflow_home = setup_airflow_home()
    
    # Step 3: Create config
    create_airflow_config(airflow_home)
    
    # Step 4: Initialize database
    if not initialize_airflow_db(airflow_home):
        print("❌ Database initialization failed")
        return False
    
    # Step 5: Create admin user
    create_admin_user(airflow_home)
    
    # Step 6: Copy DAG
    if not copy_dag_to_airflow(airflow_home):
        print("❌ DAG copy failed")
        return False
    
    # Step 7: Create connection
    create_airflow_connection(airflow_home)
    
    # Step 8: Show startup commands
    show_startup_commands(airflow_home)
    
    print("\n🎉 LOCAL AIRFLOW SETUP COMPLETE!")
    print("✅ Ready to test WIF DAG locally")
    
    return True

if __name__ == "__main__":
    main()