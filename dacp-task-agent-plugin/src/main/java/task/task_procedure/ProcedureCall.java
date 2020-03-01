package task.task_procedure;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Type;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * 调度agent调用存储过程插件
 * @author zhangqi
 *
 */
public class ProcedureCall {
	
	/**
	 * 把时间串按照响应的格式转换成日期对象
	 * @param dateStr
	 * @param format
	 * @return
	 * @throws ParseException
	 */
    public static java.sql.Date string2Date(String dateStr, String format) throws ParseException {
        SimpleDateFormat dateFormater = new SimpleDateFormat(format);
        return new java.sql.Date(dateFormater.parse(dateStr).getTime());
    }
    
    public static java.sql.Timestamp string2Timestamp(String dateStr, String format) throws Exception {
		return utilDate2Timestamp(string2Date(dateStr, format));
	}
    
    public static Timestamp utilDate2Timestamp(java.util.Date date) {
        return new java.sql.Timestamp(date == null ? new java.util.Date().getTime() : date.getTime());
    }

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		Connection conn_meta = null;
		Connection conn_procedure = null;
		boolean isCall = false;
		try {
			
			if (args.length < 2) {
				System.out.println("Usage: java -jar dacp-task-pc.jar %存储过程名称% -db %存储过程执行数据库名% -i 存储过程入参 -o 存储过程出参");
				System.exit(-1);
			}
			new ReflectSysClassLoader(System.getProperty("user.dir") + File.separator + "driver");
			
			String dbName = null;
			String procedureName = null;
			List<String> ins = new ArrayList<String>();
			List<String> outs = new ArrayList<String>();
			
			for (int i = 1; i < args.length; i++) {				
				if ("-db".equalsIgnoreCase(args[i])) {
					dbName = args[i+1];
				} else if ("-i".equalsIgnoreCase(args[i])) {
					ins.add(args[i + 1]);
				} else if ("-o".equalsIgnoreCase(args[i])) {
					outs.add(args[i + 1]);
				}
			}
			
			int paraNum = ins.size() + outs.size();
			int ti = 0;
			StringBuilder pntmp = new StringBuilder(args[0]);
			if(paraNum > 0){
				pntmp.append("(");
			}
			while(ti < paraNum){				
				if(ti < paraNum-1){
					pntmp.append("?").append(",");
				}else{
					pntmp.append("?").append(")");
				}
				ti++;			
			}
			procedureName = pntmp.toString();
			
			Properties props = new Properties();
			props.load(new FileInputStream(System.getProperty("user.dir") + File.separator + "conf" + File.separator +"system.properties"));
			
			Class.forName(props.getProperty("db.driverClass"));
			conn_meta = DriverManager.getConnection(props.getProperty("db.jdbcUrl"),props.getProperty("db.user"),props.getProperty("db.password"));
			Statement stmt = conn_meta.createStatement();			
			ResultSet rs = stmt.executeQuery("SELECT ds_acct,ds_auth,ds_conf FROM dacp_meta_datasource where ds_name='" + dbName + "' AND state=1");
			if(rs.next()){
				Type type = new TypeToken<Map<String, String>>() {}.getType();
				Map<String, String> m = new Gson().fromJson(rs.getString("ds_conf"), type);
				Class.forName(m.get("driverClassName"));
				System.out.println("即将调用存储过程:[" + procedureName + "],运行环境:" + m);
				conn_procedure = DriverManager.getConnection(m.get("url"),rs.getString("ds_acct"),AesCipher.decrypt(rs.getString("ds_auth"),Crypto.DEFAULT_SECRET_KEY));
				CallableStatement c = conn_procedure.prepareCall("{call " + procedureName + "}");				
				for(int i = 0; i < ins.size(); i++){
					String tmp[] = ins.get(i).split("\\#");
					switch (tmp[1].toLowerCase()) {
					case "varchar":
						c.setString(Integer.parseInt(tmp[0]), tmp[2]);
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					case "char":
						c.setString(Integer.parseInt(tmp[0]), tmp[2]);
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					case "date":
						c.setDate(Integer.parseInt(tmp[0]), ProcedureCall.string2Date(tmp[2], tmp[3]));
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					case "timstamp":
						c.setTimestamp(Integer.parseInt(tmp[0]), ProcedureCall.string2Timestamp(tmp[2], tmp[3]));
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					case "integer":
						c.setInt(Integer.parseInt(tmp[0]), Integer.parseInt(tmp[2]));
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					case "smallint":
						c.setInt(Integer.parseInt(tmp[0]), Integer.parseInt(tmp[2]));
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					case "varbinary":
						c.setLong(Integer.parseInt(tmp[0]), Long.parseLong(tmp[2]));
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					case "decimal":
						c.setLong(Integer.parseInt(tmp[0]), Long.parseLong(tmp[2]));
						System.out.println("第[" + tmp[0] + "]输入参数:" + tmp[2]);
						break;
					default:
						throw new Exception("不支持此IN参数类型:" + tmp[1].toLowerCase());
					}
				}

				for(int i = 0; i < outs.size(); i++){
					String tmp[] = outs.get(i).split("\\#");				
					switch (tmp[1].toLowerCase()) {
					case "varchar":
						c.registerOutParameter(Integer.parseInt(tmp[0]),java.sql.Types.VARCHAR);
						System.out.println("第[" + tmp[0] + "]输出参数:" + java.sql.Types.VARCHAR);
						break;
					case "decimal":
						c.registerOutParameter(Integer.parseInt(tmp[0]),java.sql.Types.DECIMAL);
						System.out.println("第[" + tmp[0] + "]输出参数:" + java.sql.Types.DECIMAL);
						break;
					case "varbinary":
						c.registerOutParameter(Integer.parseInt(tmp[0]),java.sql.Types.VARBINARY);
						System.out.println("第[" + tmp[0] + "]输出参数:" + java.sql.Types.VARBINARY);
						break;
					default:
						throw new Exception("不支持此OUT参数类型:" + outs.get(i).toLowerCase());
					}
				}
				c.execute();
				c.close();
				stmt.close();
				isCall = true;
            }else{
            	System.out.println("dbName is Not configured:"+dbName);
				System.exit(-1);
            }
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(conn_meta !=null && !conn_meta.isClosed()){
				conn_meta.close();
			}
			if(conn_procedure !=null && !conn_procedure.isClosed()){
				conn_procedure.close();
			}
			if(isCall){
				System.exit(0);
			}else{
				System.exit(-1);
			}
			
		}
	}

}
