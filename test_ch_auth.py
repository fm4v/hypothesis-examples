from __future__ import annotations

import dataclasses
import re
import string
import subprocess
import time
from io import StringIO

import hypothesis.strategies as st
import pandas as pd
import pytest
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, rule, Bundle, initialize, \
    run_state_machine_as_test, consumes


class CommandError(Exception):
    def __init__(self, cmd, return_code, stderr):
        self.cmd = cmd
        self.return_code = return_code
        self.stderr = stderr
        super().__init__(f"Command [{cmd}] failed with return code {return_code}:\n {stderr}")


class ChError(Exception):
    def __init__(self, sql, error_message):
        self.sql = sql
        self.error_message = error_message
        self.error_code = 0

        pattern = re.compile(r"Code:\s+(\d+)\.")
        match = pattern.search(error_message)

        if match:
            self.error_code = int(match.group(1))

        super().__init__(
            f"CH SQL [{sql}] failed with error code {self.error_code}:\n {error_message}")


def run_cmd(bash_cmd):
    result = subprocess.run(
        bash_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True)
    if result.returncode != 0:
        raise CommandError(bash_cmd, result.returncode, result.stderr.strip())

    return result.stdout.strip()


class Password:
    pass


@dataclasses.dataclass
class PlainPassword(Password):
    password: str


@dataclasses.dataclass
class NoPassword(Password):
    password = None


@dataclasses.dataclass
class NotIdentifiedPassword(Password):
    password = None


@dataclasses.dataclass
class User:
    name: str
    password: Password = None

    id: str | None = None

    def update(self, alter_user: User):
        if alter_user.name:
            self.name = alter_user.name

        if alter_user.password:
            self.password = alter_user.password

    def __hash__(self):
        return hash(self.name)


class ChClient:
    def __init__(self, user: User = None):
        self.user: User = User(name='default', password=PlainPassword(password=''))
        if user:
            self.user = user

    def exec(self, cmd, parse=False):
        psw_str = ''

        if isinstance(self.user.password, NotIdentifiedPassword):
            psw_str = ''
        if isinstance(self.user.password, NoPassword):
            psw_str = '--password ""'
        if isinstance(self.user.password, PlainPassword):
            psw_str = f'--password "{self.user.password.password}"'

        if parse:
            cmd += ' FORMAT TabSeparatedWithNames'

        print(cmd)
        try:
            result = run_cmd(
                f'clickhouse-client '
                f'-u {self.user.name} {psw_str} '
                f'-q "{cmd}"')
        except CommandError as e:
            raise ChError(cmd, e.stderr)

        if parse:
            return pd.read_csv(
                StringIO(result), sep='\t', keep_default_na=False, na_values=[]
            )
        else:
            return result.strip()

    def wait_ch_ready(self, timeout=10):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if self.exec('SELECT 1') == '1':
                    print("Connection status changed.")
                    return True
            except Exception:
                continue
            time.sleep(0.3)
        print("Timeout reached without connection status change.")
        raise Exception()

    def try_login(self):
        self.exec('SELECT 1')

    def create_user(self, user: User):
        ddl = 'CREATE USER {username} {password_ddl}'

        password_ddl = ''
        if isinstance(user.password, NotIdentifiedPassword):
            password_ddl = 'NOT IDENTIFIED'
        elif isinstance(user.password, NoPassword):
            password_ddl = 'IDENTIFIED WITH no_password'
        elif isinstance(user.password, PlainPassword):
            password_ddl = "IDENTIFIED WITH plaintext_password BY '{}'".format(
                user.password.password)

        sql = ddl.format(username=user.name, password_ddl=password_ddl)
        self.exec(sql)

    def alter_user(self, user: User, alter_user: User):
        ddl = 'ALTER USER {username} {rename_user_ddl} {password_ddl}'

        rename_user_ddl = ''
        if alter_user.name is not None and user.name != alter_user.name:
            rename_user_ddl = f'RENAME TO {alter_user.name}'

        password_ddl = ''
        if alter_user.password:
            if isinstance(alter_user.password, NotIdentifiedPassword):
                password_ddl = 'NOT IDENTIFIED'
            elif isinstance(alter_user.password, NoPassword):
                password_ddl = 'IDENTIFIED WITH no_password'
            elif isinstance(alter_user.password, PlainPassword):
                password_ddl = "IDENTIFIED WITH plaintext_password BY '{}'".format(
                    alter_user.password.password)

        sql = ddl.format(
            username=user.name,
            password_ddl=password_ddl,
            rename_user_ddl=rename_user_ddl)

        self.exec(sql)

    def drop(self, user):
        self.exec(f'DROP USER {user.name}')

    def get_users(self):
        """
        name:                 v
        id:                   f94e0f37-ecd4-aea5-bac3-2f6bed7e5ba7
        storage:              local_directory
        auth_type:            no_password
        auth_params:          {}
        host_ip:              ['::/0']
        host_names:           []
        host_names_regexp:    []
        host_names_like:      []
        default_roles_all:    1
        default_roles_list:   []
        default_roles_except: []
        grantees_any:         1
        grantees_list:        []
        grantees_except:      []
        default_database:
        :return:
        """
        return self.exec(
            "SELECT * FROM system.users WHERE name != 'default'", parse=True)

    def delete_all_users(self):
        users = self.exec(
            "SELECT * FROM system.users WHERE name != 'default'", parse=True)

        if 'nan' in users['name']:
            users.to_csv('users.csv')
            breakpoint()

        for user_name in users['name']:
            self.exec(f'DROP USER {user_name}')


@st.composite
def unique_name_strategy(draw, name_strategy=st.text(string.ascii_letters, min_size=1, max_size=2)):
    used_names = draw(st.shared(st.builds(set), key="used names"))
    name = draw(name_strategy.filter(lambda x: x not in used_names))
    used_names.add(name)
    return name


user_password_strategy = st.one_of(
    st.builds(NoPassword),
    st.builds(
        PlainPassword,
        password=st.text(alphabet=string.ascii_letters, min_size=1, max_size=10)),
)

# create user strategy
new_user = st.builds(
    User,
    name=unique_name_strategy(),
    password=user_password_strategy)

alter_user = st.builds(
    User,
    name=st.one_of(st.none(), unique_name_strategy()),
    password=st.one_of(st.none(), user_password_strategy))


class CHUserTest(RuleBasedStateMachine):
    created_user = Bundle("created_user")
    deleted_user = Bundle("deleted_user")

    @initialize()
    def init(self):
        ChClient().delete_all_users()

    @rule(
        target=created_user,
        user=st.one_of(new_user, consumes(deleted_user)),
    )
    def create_user(self, user):
        ChClient().create_user(user)
        ChClient(user).try_login()
        return user

    @rule(
        target=created_user,
        user=consumes(created_user), alter_user=alter_user
    )
    def update_user(self, user: User, alter_user: User):
        ChClient().alter_user(user, alter_user)
        user.update(alter_user)
        ChClient(user).try_login()
        return user

    @rule(target=deleted_user, user=consumes(created_user))
    def drop_user(self, user):
        ChClient().drop(user)
        with pytest.raises(ChError) as e:
            ChClient(user).try_login()
            assert e.error_code == 516
        return user

    @rule(user=st.one_of(created_user))
    def cant_create_existed_user(self, user):
        with pytest.raises(ChError) as e:
            ChClient().create_user(user)
            assert e.error_code == 493

    @rule(user=st.one_of(new_user, deleted_user), alter_user=alter_user)
    def cant_alter_non_existent_user(self, user, alter_user):
        with pytest.raises(ChError) as e:
            ChClient().alter_user(user, alter_user)
            assert e.error_code == 192

    @rule(user=st.one_of(new_user, deleted_user))
    def cant_drop_not_existing_user(self, user):
        with pytest.raises(ChError) as e:
            ChClient().drop(user)
            assert e.error_code == 192

    @rule(user=st.one_of(new_user, deleted_user))
    def cant_login_non_existent_user(self, user):
        with pytest.raises(ChError) as e:
            ChClient(user).try_login()
            assert e.error_code == 516


@pytest.fixture(scope="session", autouse=True)
def init_ch():
    # https://hub.docker.com/r/clickhouse/clickhouse-server/
    run_cmd(
        "docker run -d --name ch-test --network=host --ulimit nofile=262144:262144 "
        "clickhouse/clickhouse-server")
    ChClient().wait_ch_ready()
    yield
    run_cmd("docker rm -f ch-test")


def test_run():
    set_ = settings(
        deadline=None,
        stateful_step_count=15,
        max_examples=500
    )

    run_state_machine_as_test(CHUserTest, settings=set_)


"""
Original DDL:
CREATE USER [IF NOT EXISTS | OR REPLACE] name1 [ON CLUSTER cluster_name1]
        [, name2 [ON CLUSTER cluster_name2] ...]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password | plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [IN access_storage_type]
    [DEFAULT ROLE role [,...]]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]


Select small subset of features to test and iteratively add new features.

1. Test single user with or w/o password
`CREATE USER name`

Expected test cases:
1. Not created user can't login with correct error message
2. User can be successfully created:
    - Can login
    - Exist in system.users 
3. User can be deleted
    - Can't login anymore
    - Not exist in system.users 
4. Multiple users with different names can be created and deleted
5. User with same names can't be created
6. Recreating user
    - Create user A
    - Delete user A
    - Create user A again


3. Add password support
```
CREATE USER name   
    [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password | plaintext_password, sha256_password}] } BY 'password' ]

- NOT IDENTIFIED:               no password
- IDENTIFIED WITH no_password:  no password
- IDENTIFIED WITH plaintext_password BY 'password': use password

negative cases (random strings)

    [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password | plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']}]
    [DEFAULT DATABASE database | NONE]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]


"""
