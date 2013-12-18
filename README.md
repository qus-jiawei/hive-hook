hive-hook
=========

some hook for hive

监控hive的每个步骤中的reduce数量和输出文件的数量。

配置方式

	 <property>
		<name>hive.aux.jars.path</name>
		<value>file:///home/xxx/hive-hook-0.1-jar-with-dependencies.jar</value>
	  </property>
	  <property>
		<name>hive.exec.post.hooks</name>
		<value>cn.uc.hive.CheckJobOutputHook</value>
	  </property>

在hive的运行的配置中设定以下配置；
	
	  <property>
		<name>uc.hadoop.mail.address</name>
		<value>user1@xx.com,user2@xx.com</value>
	  </property>
	  <property>
		<name>uc.hadoop.mail.passwd</name>
		<value>xxxxx</value>
	  </property>
	
如果设定以下配置可以测试发送邮件的报警
  
	  <property>
		<name>uc.hive.debug</name>
		<value>true</value>
	  </property>
  