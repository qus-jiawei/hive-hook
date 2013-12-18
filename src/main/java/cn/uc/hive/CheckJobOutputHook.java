package cn.uc.hive;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.RunningJob;

public class CheckJobOutputHook implements ExecuteWithHookContext {

	static final private Log LOG = LogFactory
			.getLog(ExecuteWithHookContext.class.getName());
	static private String debugVar = "uc.hive.debug";
	static private String hadoopAlarmAddress = "uc.hadoop.mail.address";
	static private String hadoopMailPasswd = "uc.hadoop.mail.passwd";
	
	
	public void run(HookContext hookContext) throws Exception {
		try {
			List<TaskRunner> ctl = hookContext.getCompleteTaskList();
			HiveConf conf = hookContext.getConf();
//			LOG.info( "UCADD "+ conf.getVar(HiveConf.ConfVars.HIVEQUERYSTRING) );
			if (ctl != null) {
				for (TaskRunner tr : ctl) {
					Task<? extends Serializable> task = tr.getTask();
					if( task instanceof MapRedTask ){
						MapRedTask mrTask = (MapRedTask) task;
						MapredWork work = mrTask.getWork();
						int reduceNumber = work.getNumReduceTasks();
						Operator<? extends OperatorDesc> reducer = mrTask.getReducer();
						//获取FSoperator
						if( reduceNumber!=0 && reducer!=null ){
							FileSinkOperator fsop = getFileSinkOperator(reducer);
							if( fsop != null ){
								FileSinkDesc fsDesc = fsop.getConf();
								String outputDir = fsDesc.getDirName();
//								LOG.info("UCADD "+fsop.getName()+" "+fsDesc.getDirName() );
								compareReducerNumberAndJobOutput(reduceNumber,outputDir,conf,mrTask);
							}
							else{
								LOG.error("UCADD find some reducer havn't FileSinkOperator" );
							}
						}
						
//						if( reducer!=null) printAllOperator(reducer);
					}
				}
				alarm(conf);
			}
		} catch (Exception e) {
			LOG.error("get exception", e);
		} catch (Throwable e) {
			LOG.error("get unhandle exception", e);
		} finally {
			
		}
	}
	private void compareReducerNumberAndJobOutput(int reduceNumber,
			String outputDir,HiveConf conf,MapRedTask mrTask) throws IOException {
		Path dir = new Path(outputDir);
		FileSystem fs = dir.getFileSystem(conf);
		FileStatus[] fileStatus = fs.listStatus(dir);
		if( fileStatus.length != reduceNumber || debugOpen(conf) ){
			
			StringBuilder detail = new StringBuilder();
			detail.append("Jobid is: ").append(mrTask.getJobID()).append("\n");
			detail.append("reduce number is: ").append(reduceNumber).append("\n");
			detail.append("tmp dir is: ").append(outputDir).append("\n");
			detail.append("pathlist is:");
			for(FileStatus childFile: fileStatus){
				detail.append(",").append(childFile.getPath().getName());
			}
			detail.append("\n");
			detail.append("sql is: ").append(conf.getVar(HiveConf.ConfVars.HIVEQUERYSTRING)).append("\n");
			detail.append("\n");
			String temp = detail.toString();
			LOG.error("UCADD check is not pass!!details:\n"+ temp);
			addAlarm(temp);
		}
		else{
			LOG.info("UCADD check is pass.Jobid is "+mrTask.getJobID());
		}
	}
	private boolean debugOpen(HiveConf conf) {
		boolean debug = conf.getBoolean(debugVar, false);
		return debug;
	}
	/**
	 * 发送报警的方式
	 * @param temp
	 */
	private String text = null;
	private void alarm(HiveConf conf){
		if(text!=null){
			try {
				String passwd = conf.get(hadoopMailPasswd);
				String[] addresses = conf.getStrings(hadoopAlarmAddress);
				if( addresses != null){
					for(String address:addresses)
					SendMailHelper.sendTextEmail(passwd,address, "find hive step missing data", text);
				}
			} catch (Exception e) {
				LOG.error("send mail failed",e);
			}
		}
	}
	private void addAlarm(String detail) {
		if( text == null){
			text = detail;
		}
		else{
			text += "\n"+detail;
		}
	}
	private FileSinkOperator getFileSinkOperator(Operator<? extends OperatorDesc> operator){
//		LOG.info("UCADD "+operator.getOperatorId()+" "+operator.getOperatorName());
		if( operator instanceof FileSinkOperator ){
			if( operator.getChildOperators() != null && operator.getChildOperators().size()>0){
				LOG.error("UCADD find some FileSinkOperator has child ");
			}
			return (FileSinkOperator) operator;
		}
		else{
			List<Operator<? extends OperatorDesc>> childList = operator.getChildOperators();
			if( childList!= null ){
				for(Operator op:childList){
					FileSinkOperator re  = getFileSinkOperator(op);
					if( re != null) return re;
				}
			}
			return null;
		}
		
		
	}
//	private void printAllOperator(Operator<? extends OperatorDesc> operator) {
//		LOG.info("UCADD"+operator.getOperatorId()+" "+operator.getOperatorName());
//		List<Operator<? extends OperatorDesc>> childList = operator.getChildOperators();
//		if( childList!= null ){
//			for(Operator op:childList){
//				printAllOperator(op);
//			}
//		}	
//	}
	private void jobOutputCheck(RunningJob rj){
//		Configuration conf = rj.getConfiguration();
//		String outputDir = conf.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR);
//		LOG.info("list all output file");
		
	}
	private void debugPrintConf(Configuration conf){
		LOG.info("UCADD"+ conf.toString() );
	}
}
