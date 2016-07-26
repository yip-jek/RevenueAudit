import java.sql.SQLException;
import java.sql.Statement;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;


// Hive 代理
public class HiveAgentTest {
	private static final String JDBC_DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";

	private static final String HIVE2_CONN_HEAD = "jdbc:hive2://";
	private static final String HIVE2_CONN_TAIL = "/default";

	private static String HostInfo = null;
	private static String User     = null;
	private static String Password = null;

	private StringBuffer m_errorMsg;			// 错误信息
	private boolean m_isConnected = false;		// 是否已连接
	private Connection m_conn = null;

	public void SetZooKeeperQuorum(String zk_quorum)
	{
		HostInfo = zk_quorum;
		System.out.println("[HIVE_AGENT] [TEST] Set ZooKeeperQuorum = "+HostInfo);
	}

	public void SetKrb5Conf(String krb5_conf)
	{
		System.out.println("[HIVE_AGENT] [TEST] Set krb5.conf OK!");
	}

	public void SetUserKeytab(String usr_keytab)
	{
		User = usr_keytab;
		System.out.println("[HIVE_AGENT] [TEST] Set user.keytab = "+User);
	}

	public void SetPrincipal(String principal)
	{
		Password = principal;
		System.out.println("[HIVE_AGENT] [TEST] Set principal = "+Password);
	}

	public void SetJaasConf(String jaas_conf)
	{
		System.out.println("[HIVE_AGENT] [TEST] Set jaas.conf OK!");
	}

	// 初始化
	public boolean Init()
	{
		// For test
		return true;
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
		conn_buf.append(HostInfo);
		conn_buf.append(HIVE2_CONN_TAIL);

		final String CONN_STR = conn_buf.toString();
		System.out.println("[HIVE_AGENT] Connect to: "+CONN_STR);

		try
		{
			Class.forName(JDBC_DRIVER_CLASS_NAME);

			// 进行连接
			m_conn = DriverManager.getConnection(CONN_STR, User, Password);

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
