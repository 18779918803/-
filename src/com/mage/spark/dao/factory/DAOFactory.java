package com.mage.spark.dao.factory;

import com.mage.spark.dao.IAreaDao;
import com.mage.spark.dao.ICarTrackDAO;
import com.mage.spark.dao.IMonitorDAO;
import com.mage.spark.dao.IRandomExtractDAO;
import com.mage.spark.dao.ITaskDAO;
import com.mage.spark.dao.IWithTheCarDAO;
import com.mage.spark.dao.impl.AreaDaoImpl;
import com.mage.spark.dao.impl.CarTrackDAOImpl;
import com.mage.spark.dao.impl.MonitorDAOImpl;
import com.mage.spark.dao.impl.RandomExtractDAOImpl;
import com.mage.spark.dao.impl.TaskDAOImpl;
import com.mage.spark.dao.impl.WithTheCarDAOImpl;

/**
 * DAO工厂类
 * @author root
 *
 */
public class DAOFactory {
	
	
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	public static IMonitorDAO getMonitorDAO(){
		return new MonitorDAOImpl();
	}
	
	public static IRandomExtractDAO getRandomExtractDAO(){
		return new RandomExtractDAOImpl();
	}
	
	public static ICarTrackDAO getCarTrackDAO(){
		return new CarTrackDAOImpl();
	}
	
	public static IWithTheCarDAO getWithTheCarDAO(){
		return new WithTheCarDAOImpl();
	}

	public static IAreaDao getAreaDao() {
		return  new AreaDaoImpl();
		
	}
}
