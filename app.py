# app.py
# Main Flask application file

from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user, login_required
from ldap3 import Server, Connection, ALL
from datetime import datetime
import os
from confluent_kafka.admin import AdminClient, NewTopic, AclBinding, AclOperation, AclPermissionType, ResourceType, ResourcePatternType
from confluent_kafka import KafkaException
import concurrent.futures

# --- Configuration ---
# Updated with the exact settings from your working configuration file.
LDAP_SERVER = 'ldapnode.infra.alephys.com'
BASE_DN = 'dc=alephys,dc=com' # Corrected BASE_DN from your config
LDAP_ADMIN_GROUP_CN = 'admins'

# Using the principal and credentials from your working config file
LDAP_BIND_USER_DN = 'uid=gpasupuleti,cn=users,cn=accounts,dc=alephys,dc=com'
LDAP_BIND_USER_PASSWORD = '4lph@1234'

CONFLUENT_COMMANDS_CONFIG_FILE = 'client.properties' # File with bootstrap.servers and security protocol details

# --- App Initialization ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///kafka_requests.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# --- Models ---
class User(UserMixin):
    def __init__(self, username, is_admin=False):
        self.id = username
        self.is_admin = is_admin

class Request(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), nullable=False)
    app_code = db.Column(db.String(50), nullable=False)
    request_type = db.Column(db.String(50), nullable=False)
    details = db.Column(db.String(500), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='Pending') # Pending, Approved, Rejected, Executed, Failed
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    approved_by = db.Column(db.String(100), nullable=True)
    execution_logs = db.Column(db.Text, nullable=True)

    def __repr__(self):
        return f'<Request {self.id} - {self.request_type} for {self.app_code}>'

# --- LDAP Authentication ---
def authenticate_ldap(username, password):
    """
    Authenticates a user against FreeIPA LDAP using a bind user.
    """
    if not password:
        return False, False
    server = Server(LDAP_SERVER, get_info=ALL)
    try:
        with Connection(server, user=LDAP_BIND_USER_DN, password=LDAP_BIND_USER_PASSWORD, auto_bind=True) as conn:
            search_base = f'cn=users,cn=accounts,{BASE_DN}'
            search_filter = f'(uid={username})'
            conn.search(search_base, search_filter)
            if not conn.entries:
                print(f"LDAP Search: User '{username}' not found.")
                return False, False
            user_dn = conn.entries[0].entry_dn
            try:
                with Connection(server, user=user_dn, password=password, auto_bind=True) as user_conn:
                    is_authenticated = user_conn.bound
            except Exception:
                is_authenticated = False
            if not is_authenticated:
                print(f"LDAP Auth: Invalid password for user '{username}'.")
                return False, False
            is_admin = False
            group_search_base = f'cn=groups,cn=accounts,{BASE_DN}'
            group_search_filter = f'(&(objectClass=ipausergroup)(cn={LDAP_ADMIN_GROUP_CN})(member={user_dn}))'
            conn.search(group_search_base, group_search_filter)
            if conn.entries:
                is_admin = True
            return is_authenticated, is_admin
    except Exception as e:
        print(f"LDAP Authentication Error: {e}")
        return False, False

@login_manager.user_loader
def load_user(user_id):
    is_admin = session.get('is_admin', False)
    return User(user_id, is_admin)

# --- Kafka Admin Client API Integration ---
def parse_client_properties(file_path):
    """ Reads the client.properties file and returns a config dictionary. """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CRITICAL ERROR: The Kafka configuration file '{file_path}' was not found in the application directory.")
    config = {}
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip() and not line.startswith('#') and '=' in line:
                key, value = line.strip().split('=', 1)
                config[key.strip()] = value.strip()
    return config

def get_admin_client():
    """ Creates and returns a Confluent Kafka AdminClient instance. """
    try:
        config = parse_client_properties(CONFLUENT_COMMANDS_CONFIG_FILE)
        print(f"Attempting to create AdminClient with config: {config}")
        return AdminClient(config)
    except FileNotFoundError as e:
        print(f"Error creating AdminClient: {e}")
        raise e
    except Exception as e:
        print(f"Error creating AdminClient: {e}")
        raise KafkaException(f"Failed to create Kafka AdminClient. Is 'bootstrap.servers' in '{CONFLUENT_COMMANDS_CONFIG_FILE}' correct and reachable?")

def execute_kafka_request(req):
    """
    Takes a request object and executes the corresponding Kafka command using the AdminClient API.
    """
    try:
        admin_client = get_admin_client()
    except Exception as e:
        print(f"Execution failed: {e}")
        return False, str(e)

    details = dict(item.split("=") for item in req.details.split(";"))
    
    try:
        if req.request_type == 'create_topic':
            topic_name = f"{req.app_code.lower()}_{details['topic_name']}"
            partitions = int(details.get('partitions', '3'))
            replication = int(details.get('replication', '3'))
            new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication)
            fs = admin_client.create_topics([new_topic])
            fs[topic_name].result()
            logs = f"Successfully created topic: {topic_name}"
            return True, logs

        elif req.request_type == 'producer_acl':
            topic_name = f"{req.app_code.lower()}_{details['topic_name']}"
            principal = f"User:{details['principal']}"
            acl_bindings = [
                AclBinding(ResourceType.TOPIC, topic_name, ResourcePatternType.LITERAL, principal, '*', AclOperation.WRITE, AclPermissionType.ALLOW),
                AclBinding(ResourceType.TOPIC, topic_name, ResourcePatternType.LITERAL, principal, '*', AclOperation.CREATE, AclPermissionType.ALLOW)
            ]
            fs_map = admin_client.create_acls(acl_bindings)
            for binding, future in fs_map.items():
                future.result()
            logs = f"Successfully created Producer ACLs (WRITE, CREATE) for principal '{principal}' on topic '{topic_name}'."
            return True, logs

        elif req.request_type == 'consumer_acl':
            topic_name = f"{req.app_code.lower()}_{details['topic_name']}"
            principal = f"User:{details['principal']}"
            consumer_group = details.get('consumer_group', '')
            if not consumer_group:
                return False, "Validation Error: Consumer Group ID cannot be empty for a consumer ACL request."
            acl_bindings = [
                AclBinding(ResourceType.TOPIC, topic_name, ResourcePatternType.LITERAL, principal, '*', AclOperation.READ, AclPermissionType.ALLOW),
                AclBinding(ResourceType.GROUP, consumer_group, ResourcePatternType.LITERAL, principal, '*', AclOperation.READ, AclPermissionType.ALLOW)
            ]
            fs_map = admin_client.create_acls(acl_bindings)
            for binding, future in fs_map.items():
                future.result()
            logs = f"Successfully created Consumer ACLs (READ on Topic & Group) for principal '{principal}' on topic '{topic_name}' and group '{consumer_group}'."
            return True, logs
            
        elif req.request_type == 'consumer_group_acl':
            principal = f"User:{details['principal']}"
            consumer_group = details.get('consumer_group', '')
            if not consumer_group:
                return False, "Validation Error: Consumer Group ID cannot be empty for this request."
            acl_bindings = [
                AclBinding(ResourceType.GROUP, consumer_group, ResourcePatternType.LITERAL, principal, '*', AclOperation.READ, AclPermissionType.ALLOW)
            ]
            fs_map = admin_client.create_acls(acl_bindings)
            for binding, future in fs_map.items():
                future.result()
            logs = f"Successfully created Consumer Group ACL for principal '{principal}' on group '{consumer_group}'."
            return True, logs

        else:
            return False, "Unsupported request type."

    except KafkaException as e:
        error_message = f"Kafka API Error: {e}"
        print(error_message)
        return False, error_message
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        return False, error_message

# --- Routes ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        is_authenticated, is_admin = authenticate_ldap(username, password)
        if is_authenticated:
            user = User(username, is_admin)
            login_user(user)
            session['is_admin'] = is_admin
            flash('Logged in successfully.', 'success')
            return redirect(url_for('admin_dashboard') if is_admin else url_for('user_dashboard'))
        else:
            flash('Invalid username or password.', 'danger')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    return redirect(url_for('admin_dashboard') if current_user.is_admin else url_for('user_dashboard'))

@app.route('/dashboard')
@login_required
def user_dashboard():
    if current_user.is_admin:
        return redirect(url_for('admin_dashboard'))
    user_requests = Request.query.filter_by(username=current_user.id).order_by(Request.created_at.desc()).all()
    return render_template('user_dashboard.html', requests=user_requests)

@app.route('/request/new', methods=['GET', 'POST'])
@login_required
def new_request():
    if request.method == 'POST':
        app_code = request.form['app_code']
        request_type = request.form['request_type']
        details_list = []
        if request_type == 'create_topic':
            details_list.append(f"topic_name={request.form['topic_name']}")
            details_list.append(f"partitions={request.form['partitions']}")
            details_list.append(f"replication={request.form['replication']}")
        elif request_type == 'producer_acl':
            details_list.append(f"topic_name={request.form['acl_topic_name']}")
            details_list.append(f"principal={request.form['principal']}")
        elif request_type == 'consumer_acl':
            details_list.append(f"topic_name={request.form['acl_topic_name']}")
            details_list.append(f"principal={request.form['principal']}")
            details_list.append(f"consumer_group={request.form['consumer_group']}")
        elif request_type == 'consumer_group_acl':
            details_list.append(f"topic_name=N/A") # No topic name needed
            details_list.append(f"principal={request.form['principal']}")
            details_list.append(f"consumer_group={request.form['consumer_group']}")

        details_str = ";".join(details_list)
        new_req = Request(username=current_user.id, app_code=app_code, request_type=request_type, details=details_str)
        db.session.add(new_req)
        db.session.commit()
        flash('Your request has been submitted for approval.', 'success')
        return redirect(url_for('user_dashboard'))
    return render_template('new_request.html')

@app.route('/request/<int:request_id>')
@login_required
def view_request(request_id):
    req = Request.query.get_or_404(request_id)
    if not current_user.is_admin and req.username != current_user.id:
        flash('You are not authorized to view this request.', 'danger')
        return redirect(url_for('user_dashboard'))
    details = dict(item.split("=") for item in req.details.split(";"))
    return render_template('view_request.html', request=req, details=details)

@app.route('/admin')
@login_required
def admin_dashboard():
    if not current_user.is_admin:
        flash('You do not have permission to access the admin page.', 'danger')
        return redirect(url_for('user_dashboard'))
    pending_requests = Request.query.filter_by(status='Pending').order_by(Request.created_at.asc()).all()
    processed_requests = Request.query.filter(Request.status != 'Pending').order_by(Request.created_at.desc()).limit(50).all()
    return render_template('admin_dashboard.html', pending=pending_requests, processed=processed_requests)

@app.route('/admin/request/<int:request_id>/approve', methods=['POST'])
@login_required
def approve_request(request_id):
    if not current_user.is_admin:
        return redirect(url_for('user_dashboard'))
    req = Request.query.get_or_404(request_id)
    if req.status == 'Pending':
        req.approved_by = current_user.id
        success, logs = execute_kafka_request(req)
        req.execution_logs = logs
        req.status = 'Executed' if success else 'Failed'
        db.session.commit()
        flash(f'Request {req.id} has been processed. Status: {req.status}', 'success' if success else 'danger')
    else:
        flash(f'Request {req.id} is not in a pending state.', 'warning')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/request/<int:request_id>/reject', methods=['POST'])
@login_required
def reject_request(request_id):
    if not current_user.is_admin:
        return redirect(url_for('user_dashboard'))
    req = Request.query.get_or_404(request_id)
    if req.status == 'Pending':
        req.status = 'Rejected'
        req.approved_by = current_user.id
        db.session.commit()
        flash(f'Request {req.id} has been rejected.', 'info')
    else:
        flash(f'Request {req.id} is not in a pending state.', 'warning')
    return redirect(url_for('admin_dashboard'))

# --- Main Execution ---
if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, host='0.0.0.0', port=5001)
