import java.sql.SQLException;
import java.sql.Statement;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;


// Hive 代理
public class HiveAgent {
	private static final String JDBC_DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";
	
	private static final String SECURITY_KRB5_CONF = "java.security.krb5.conf";
	private static final String SECURITY_AUTH_LOGIN = "java.security.auth.login.config";
	private static final String KEYTAB_FILE_KEY = "username.client.keytab.file";
	private static final String USER_NAME_KEY = "username.client.kerberos.principal";

	private static final String HIVE2_CONN_HEAD = "jdbc:hive2://";
	private static final String HIVE2_CONN_TAIL = ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM;";

	private static String STR_ZK_QUORUM = null;
	private static String STR_KRB5_CONF = null;
	private static String STR_USER_KEYTAB = null;
	private static String STR_PRINCIPAL = null;
	private static String STR_JAAS_CONF = null;
	private static Configuration HADOOP_CONF = null;

	private StringBuffer m_errorMsg;			// 错误信息
	private boolean m_isConnected = false;		// 是否已连接
	private Connection m_conn = null;

	public void SetZooKeeperQuorum(String zk_quorum)
	{
		STR_ZK_QUORUM = zk_quorum;
		System.out.println("[HIVE_AGENT] Set ZooKeeperQuorum: "+STR_ZK_QUORUM);
	}

	public void SetKrb5Conf(String krb5_conf)
	{
		STR_KRB5_CONF = krb5_conf;
		System.out.println("[HIVE_AGENT] Set krb5.conf: "+STR_KRB5_CONF);
	}

	public void SetUserKeytab(String usr_keytab)
	{
		STR_USER_KEYTAB = usr_keytab;
		System.out.println("[HIVE_AGENT] Set user.keytab: "+STR_USER_KEYTAB);
	}

	public void SetPrincipal(String principal)
	{
		STR_PRINCIPAL = principal;
		System.out.println("[HIVE_AGENT] Set principal: "+STR_PRINCIPAL);
	}

	public void SetJaasConf(String jaas_conf)
	{
		STR_JAAS_CONF = jaas_conf;
		System.out.println("[HIVE_AGENT] Set jaas.conf: "+STR_JAAS_CONF);
	}

	// 初始化
	public boolean Init()
	{
		System.setProperty(SECURITY_KRB5_CONF, STR_KRB5_CONF);
		System.setProperty(SECURITY_AUTH_LOGIN, STR_JAAS_CONF);

		HADOOP_CONF = new Configuration();
		HADOOP_CONF.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
		HADOOP_CONF.set(HADOOP_SECURITY_AUTHORIZATION, "true");
		HADOOP_CONF.set(KEYTAB_FILE_KEY, STR_USER_KEYTAB);
		HADOOP_CONF.set(USER_NAME_KEY, STR_PRINCIPAL);

		// 进行登录认证
		UserGroupInformation.setConfiguration(HADOOP_CONF);
		try
		{
			SecurityUtil.login(HADOOP_CONF, KEYTAB_FILE_KEY, USER_NAME_KEY);
			System.out.println("[HIVE_AGENT] [INIT] Login OK.");
			return true;
		}
		catch (IOException io_ex)
		{
			io_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] [INIT] Login failed: [IOException] ");
			m_errorMsg.append(io_ex);
			System.out.println(m_errorMsg);
			return false;
		}
	}

	// 是否已经连接
	public boolean IsConnected()
	{
		return m_isConnected;
	}

	// 返回最后一次的错误信息
	public String GetErrorMsg()
	{
		return m_errorMsg.toString();
	}

	// 连接 Hive
	// 返回：0-成功，1-已经连接，-1-连接失败(ClassNotFoundException)，-2-连接失败(SQLException), -3-连接失败(NullPointerException)
	public int Connect()
	{
		// 已连接
		if ( m_isConnected )
		{
			m_errorMsg = new StringBuffer("[HIVE_AGENT] Already connected!");
			System.out.println(m_errorMsg);
			return 1;
		}

		// 组织连接字串
		StringBuffer conn_buf = new StringBuffer(HIVE2_CONN_HEAD);
		conn_buf.append(STR_ZK_QUORUM);
		conn_buf.append(HIVE2_CONN_TAIL);

		final String CONN_STR = conn_buf.toString();
		System.out.println("[HIVE_AGENT] Connect to: "+CONN_STR);

		try
		{
			Class.forName(JDBC_DRIVER_CLASS_NAME);

			// 进行连接
			m_conn = DriverManager.getConnection(CONN_STR, "", "");

			// 连接成功！
			m_isConnected = true;
			System.out.println("[HIVE_AGENT] Connect OK.");
			return 0;
		}
		catch ( ClassNotFoundException not_found_ex )
		{
			not_found_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] Connect failed: [ClassNotFoundException] ");
			m_errorMsg.append(not_found_ex);
			System.out.println(m_errorMsg);
			return -1;
		}
		catch ( SQLException sql_ex )
		{
			sql_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] Connect failed: [SQLException] ");
			m_errorMsg.append(sql_ex);
			System.out.println(m_errorMsg);
			return -2;
		}
		catch ( NullPointerException np_ex )
		{
			np_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] Connect failed: [NullPointerException] ");
			m_errorMsg.append(np_ex);
			System.out.println(m_errorMsg);
			return -3;
		}
	}

	// 断开连接
	// 返回：true-成功，false-失败
	public boolean Disconnect()
	{
		// 未连接
		if ( !m_isConnected )
		{
			m_errorMsg = new StringBuffer("[HIVE_AGENT] Not connected!");
			System.out.println(m_errorMsg);
			return false;
		}

		try
		{
			m_conn.close();

			m_isConnected = false;
			System.out.println("[HIVE_AGENT] Disconnect OK.");
			return true;
		}
		catch ( SQLException sql_ex )
		{
			sql_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] Disconnect failed: [SQLException] ");
			m_errorMsg.append(sql_ex);
			System.out.println(m_errorMsg);
			return false;
		}
		catch ( NullPointerException np_ex )
		{
			np_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] Disconnect failed: [NullPointerException] ");
			m_errorMsg.append(np_ex);
			System.out.println(m_errorMsg);
			return false;
		}
	}
	
	// 执行 HIVE SQL 语句
	// 返回：true-成功，false-执行失败
	public boolean ExecuteSQL(String sql)
	{
		try
		{
			System.out.println("[HIVE_AGENT] Create statement ...");
			Statement st = m_conn.createStatement();

			System.out.println("[HIVE_AGENT] Execute sql: "+sql);
			st.execute(sql);

			// 释放 Statement 资源
			System.out.println("[HIVE_AGENT] Release statement ...");
			st.close();

			System.out.println("[HIVE_AGENT] Execute sql OK.");
			return true;
		}
		catch ( SQLException sql_ex )
		{
			sql_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] Execute sql failed: [SQLException] ");
			m_errorMsg.append(sql_ex);
			System.out.println(m_errorMsg);
			return false;
		}
	}

	// 获取 HIVE 数据
	// 返回：结果数组
	public ArrayList<String> FetchData(String sql)
	{
		try
		{
			System.out.println("[HIVE_AGENT] Create statement ...");
			Statement st = m_conn.createStatement();

			StringBuffer buf_str = new StringBuffer("[HIVE_AGENT] Fetch data sql: ");
			buf_str.append(sql);
			System.out.println(buf_str);

			ResultSet rs = st.executeQuery(sql);

			// 第一行数据
			int column_size = 0;
			if ( rs.next() )
			{
				// 获得列数
				ResultSetMetaData rsmd = rs.getMetaData();
				column_size = rsmd.getColumnCount();
			}
			else	// 无数据
			{
				m_errorMsg = new StringBuffer("[HIVE_AGENT] Fetch data failed: NO data! [data size: 0]");
				System.out.println(m_errorMsg);
				return new ArrayList<String>();
			}

			int total_size = 0;
			ArrayList<String> data_array = new ArrayList<String>();
			do
			{
				// 统计总行数
				++total_size;

				for ( int i = 1; i <= column_size; ++i )
				{
					if ( i > 1 )
					{
						// 列之间用<TAB>键分隔
						buf_str.append("\t");
						buf_str.append(rs.getString(i));
					}
					else
					{
						// 先new一个默认的StringBuffer对象，再将rs.getString返回的结果append进去
						// 这么做的原因是rs.getString可能返回null（空），append操作会将null转换为"null"(字符串)
						buf_str = new StringBuffer();
						buf_str.append(rs.getString(i));
					}
				}

				data_array.add(buf_str.toString());
			} while ( rs.next() );

			// 释放 Statement 资源
			System.out.println("[HIVE_AGENT] Release statement ...");
			st.close();

			buf_str = new StringBuffer("[HIVE_AGENT] Fetch data size: ");
			buf_str.append(total_size);
			System.out.println(buf_str);

			System.out.println("[HIVE_AGENT] Fetch data OK.");
			return data_array;
		}
		catch ( SQLException sql_ex )
		{
			sql_ex.printStackTrace();

			m_errorMsg = new StringBuffer("[HIVE_AGENT] Fetch data failed: [SQLException] ");
			m_errorMsg.append(sql_ex);
			System.out.println(m_errorMsg);

			return new ArrayList<String>();
		}
	}
}
