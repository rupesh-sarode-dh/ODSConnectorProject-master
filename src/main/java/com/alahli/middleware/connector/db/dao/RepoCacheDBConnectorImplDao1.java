package com.alahli.middleware.connector.db.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

import org.apache.camel.Exchange;

import org.apache.camel.language.simple.Simple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.alahli.middleware.logging.AppLogger;
import com.alahli.middleware.utility.Utils.DateUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Lazy
@Component
public class RepoCacheDBConnectorImplDao1 {

	static Logger cachedbLogger = LoggerFactory.getLogger(RepoCacheDBConnectorImplDao1.class);

	@Autowired(required = false)
	private DataSource cacheDataSource;

	@Autowired
	ObjectMapper mapper;

	@Autowired(required = false)
	AppLogger oAppLogger;

	@Autowired
	DateUtil oDateUtils;

	/**
	 * this api is giving process master and related process params master data.
	 * 
	 * @param projectCode
	 * @param processCode
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */

	public JsonNode GetProcessConfig(@Simple("${body[ProcessConfigRequest][projectCode]}") String projectCode,
			@Simple("${body[ProcessConfigRequest][processCode]}") String processCode, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {
			conn = cacheDataSource.getConnection();
			System.out.println("after connection" + cacheDataSource.getConnection());
			String query = "SELECT PROJECT_CODE,PROCESS_CODE,PROCESS_DESCRIPTION,CACHE_TIME,STATUS,TIME_SPECIFIC_CACHE,ERROR_COUNT,CACHE_TIME_UNIT,ERROR_TIME,ERROR_TIME_UNIT, ID PROCESS_ID,\r\n"
					+ "TO_CHAR(NULL) PARAM_CODE,TO_CHAR(NULL) PARAM_XPATH, TO_CHAR(NULL) PARAM_TYPE, TO_CHAR(NULL) PARAM_FORMAT, TO_NUMBER(0) PARAM_ORDER, TO_CHAR(NULL) IS_CACHE_VARIABLE, TO_CHAR('PM') IDENTIFIER FROM DH_REPO_CACHE_PROCESS_MASTER mcpm where PROJECT_CODE=:PROJECT_CODE and PROCESS_CODE=:PROCESS_CODE \r\n"
					+ "UNION\r\n"
					+ "SELECT TO_CHAR(NULL) PROJECT_CODE,TO_CHAR(NULL) PROCESS_CODE,TO_CHAR(NULL) PROCESS_DESCRIPTION,TO_NUMBER(NULL) CACHE_TIME,TO_CHAR(NULL) STATUS,TO_CHAR(NULL) TIME_SPECIFIC_CACHE,TO_NUMBER(NULL) ERROR_COUNT,TO_CHAR(NULL) CACHE_TIME_UNIT,TO_NUMBER(NULL) ERROR_TIME,TO_CHAR(NULL) ERROR_TIME_UNIT, PROCESS_CODE PROCESS_ID, PARAM_CODE,PARAM_XPATH, PARAM_TYPE, PARAM_FORMAT, PARAM_ORDER, IS_CACHE_VARIABLE, TO_CHAR('PPM') IDENTIFIER FROM DH_REPO_CACHE_PROCESS_PARAMS where PROCESS_CODE =( select ID from DH_REPO_CACHE_PROCESS_MASTER where PROJECT_CODE=:PROJECT_CODE and PROCESS_CODE=:PROCESS_CODE) ORDER BY PARAM_ORDER";
			oPreparedStatement = conn.prepareStatement(query);
			oPreparedStatement.setString(1, projectCode);
			oPreparedStatement.setString(2, processCode);
			oPreparedStatement.setString(3, projectCode);
			oPreparedStatement.setString(4, processCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessConfigRequest", "ProcessConfigRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			rs = oPreparedStatement.executeQuery();

			ResultSetMetaData rsMetadata = null;
			rsMetadata = rs.getMetaData();
			int noOfColumns = rsMetadata.getColumnCount();

			ObjectNode oProcessConfigObjectNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessConfigResponse = oProcessConfigObjectNode.putObject("ProcessConfigResponse");
			ObjectNode oProcessObjNode = oProcessConfigResponse.putObject("process");
			ObjectNode oProcessParamsObjNode = oProcessConfigResponse.putObject("processParams");
			ArrayNode processParamArryNode = oProcessParamsObjNode.putArray("processParam");

			while (rs.next()) {
//				System.out.println("RS TEMP"+rs.getString(0));
				ObjectNode tempObjNode = null;
				String identifier = rs.getString("IDENTIFIER");
				if (null != identifier && identifier.equals("PM")) // process master
					tempObjNode = oProcessObjNode;
				else {
					tempObjNode = JsonNodeFactory.instance.objectNode();
					processParamArryNode.add(tempObjNode); // process params master
				}

				for (int i = 1; i <= noOfColumns; i++) {

					String columnName = rsMetadata.getColumnName(i);
					String columnValue = rs.getString(i);

					tempObjNode.put(columnName, columnValue);
				}

			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessConfigResponse", "ProcessConfigResponse", null,
						oProcessConfigObjectNode.toString(), exchange);
			}

			return oProcessConfigObjectNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this method meant to get data from PROCESS_DATA table based on cacheKey and
	 * processCode
	 * 
	 * @param cacheKey
	 * @param processCode
	 * @return
	 */

	public JsonNode GetCache(@Simple("${body[GetCacheRequest][CACHE_KEY]}") String cacheKey,
			@Simple("${body[GetCacheRequest][PROCESS_CODE]}") Integer processCode, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {

			conn = cacheDataSource.getConnection();

			String query = "SELECT CACHE_KEY,PROCESS_CODE,PARAM_CODE1,PARAM_VALUE1,PARAM_CODE2,PARAM_VALUE2,PARAM_CODE3,PARAM_VALUE3,PARAM_CODE4,PARAM_VALUE4,PARAM_CODE5,PARAM_VALUE5,PARAM_CODE6,PARAM_VALUE6,PARAM_CODE7,\r \n"
					+ "PARAM_VALUE7,PARAM_CODE8,PARAM_VALUE8,PARTITION_MONTH,STATUS,RESPONSE_OBJECT FROM DH_REPO_CACHE_PROCESS_DATA where CACHE_KEY= ? AND PROCESS_CODE=? ";

			oPreparedStatement = conn.prepareStatement(query);
			oPreparedStatement.setString(1, cacheKey);
			oPreparedStatement.setInt(2, processCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("GetCacheRequest", "GetCacheRequest", exchange.getIn().getBody(String.class),
						null, exchange);
			}

			rs = oPreparedStatement.executeQuery();

			ObjectNode oGetCacheResponseObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oCacheDataResponseObjNode = oGetCacheResponseObjNode.putObject("GetCacheResponse");

			while (rs.next()) {

				oCacheDataResponseObjNode.put("CACHE_KEY", rs.getString(1));
				oCacheDataResponseObjNode.put("PROCESS_CODE", rs.getInt(2));
				oCacheDataResponseObjNode.put("PARAM_CODE1", rs.getString(3));
				oCacheDataResponseObjNode.put("PARAM_VALUE1", rs.getString(4));
				oCacheDataResponseObjNode.put("PARAM_CODE2", rs.getString(5));
				oCacheDataResponseObjNode.put("PARAM_VALUE2", rs.getString(6));
				oCacheDataResponseObjNode.put("PARAM_CODE3", rs.getString(7));
				oCacheDataResponseObjNode.put("PARAM_VALUE3", rs.getString(8));
				oCacheDataResponseObjNode.put("PARAM_CODE4", rs.getString(9));
				oCacheDataResponseObjNode.put("PARAM_VALUE4", rs.getString(10));
				oCacheDataResponseObjNode.put("PARAM_CODE5", rs.getString(11));
				oCacheDataResponseObjNode.put("PARAM_VALUE5", rs.getString(12));
				oCacheDataResponseObjNode.put("PARAM_CODE6", rs.getString(13));
				oCacheDataResponseObjNode.put("PARAM_VALUE6", rs.getString(14));
				oCacheDataResponseObjNode.put("PARAM_CODE7", rs.getString(15));
				oCacheDataResponseObjNode.put("PARAM_VALUE7", rs.getString(16));
				oCacheDataResponseObjNode.put("PARAM_CODE8", rs.getString(17));
				oCacheDataResponseObjNode.put("PARAM_VALUE8", rs.getString(18));
				oCacheDataResponseObjNode.put("PARTITION_MONTH", rs.getString(19));
				oCacheDataResponseObjNode.put("STATUS", rs.getString(20));
				oCacheDataResponseObjNode.put("RESPONSE_OBJECT", rs.getString(21));

			}

			oGetCacheResponseObjNode.set("GetCacheResponse", oCacheDataResponseObjNode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("GetCacheResponse", "GetCacheResponse", null,
						oGetCacheResponseObjNode.toString(), exchange);
			}

			return oGetCacheResponseObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}

		return null;
	}

	/***
	 * This method meant to insert cache data to audit table.
	 * 
	 * @param processDataRef
	 * @param paramCode1
	 * @param paramValue1
	 * @param paramCode2
	 * @param paramValue2
	 * @param paramCode3
	 * @param paramValue3
	 * @param paramCode4
	 * @param paramValue4
	 * @param paramCode5
	 * @param paramValue5
	 * @param paramCode6
	 * @param paramValue6
	 * @param paramCode7
	 * @param paramValue7
	 * @param paramCode8
	 * @param paramValue8
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode AddCacheAudit(@Simple("${body[CacheAuditRequest][PROCESS_CODE]}") Integer processCode,
			@Simple("${body[CacheAuditRequest][CODE]}") String code,
			@Simple("${body[CacheAuditRequest][DESCRIPTION]}") String description,
			@Simple("${body[CacheAuditRequest][AUDIT_TIME]}") String auditTime,
			@Simple("${body[CacheAuditRequest][PROCESS_DATA_REF]}") Integer processDataRef,
			@Simple("${body[CacheAuditRequest][PARAM_CODE1]}") String paramCode1,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE1]}") String paramValue1,
			@Simple("${body[CacheAuditRequest][PARAM_CODE2]}") String paramCode2,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE2]}") String paramValue2,
			@Simple("${body[CacheAuditRequest][PARAM_CODE3]}") String paramCode3,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE3]}") String paramValue3,
			@Simple("${body[CacheAuditRequest][PARAM_CODE4]}") String paramCode4,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE4]}") String paramValue4,
			@Simple("${body[CacheAuditRequest][PARAM_CODE5]}") String paramCode5,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE5]}") String paramValue5,
			@Simple("${body[CacheAuditRequest][PARAM_CODE6]}") String paramCode6,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE6]}") String paramValue6,
			@Simple("${body[CacheAuditRequest][PARAM_CODE7]}") String paramCode7,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE7]}") String paramValue7,
			@Simple("${body[CacheAuditRequest][PARAM_CODE8]}") String paramCode8,
			@Simple("${body[CacheAuditRequest][PARAM_VALUE8]}") String paramValue8, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {

			conn = cacheDataSource.getConnection();

			String insertQuery = "INSERT INTO DH_REPO_CACHE_PROCESS_AUDIT(PROCESS_CODE,CODE,DESCRIPTION,AUDIT_TIME,PROCESS_DATA_REF,PARAM_CODE1,PARAM_VALUE1,PARAM_CODE2,PARAM_VALUE2,PARAM_CODE3,PARAM_VALUE3,PARAM_CODE4,PARAM_VALUE4,PARAM_CODE5,PARAM_VALUE5,PARAM_CODE6,PARAM_VALUE6,\n"
					+ "PARAM_CODE7,PARAM_VALUE7,PARAM_CODE8,PARAM_VALUE8)\n"
					+ "VALUES(:PROCESS_CODE,:CODE,:DESCRIPTION,:AUDIT_TIME,:PROCESS_DATA_REF,:PARAM_CODE1,:PARAM_VALUE1,:PARAM_CODE2,:PARAM_VALUE2,:PARAM_CODE3,:PARAM_VALUE3,:PARAM_CODE4,:PARAM_VALUE4,:PARAM_CODE5,:PARAM_VALUE5,:PARAM_CODE6,:PARAM_VALUE6,:\n"
					+ "PARAM_CODE7,:PARAM_VALUE7,:PARAM_CODE8,:PARAM_VALUE8)";

			oPreparedStatement = conn.prepareStatement(insertQuery.toString(), new int[] { 1 }); // new int[] { 1 }
																									// means capturing
																									// first column
			// index value which is ID
			oPreparedStatement.setInt(1, processCode);
			oPreparedStatement.setString(2, code);
			oPreparedStatement.setString(3, description);
			oPreparedStatement.setDate(4,
					java.sql.Date.valueOf(oDateUtils.convertToLocalDate(auditTime, "dd-MM-yyyy")));
			oPreparedStatement.setInt(5, processDataRef);
			oPreparedStatement.setString(6, paramCode1);
			oPreparedStatement.setString(7, paramValue1);
			oPreparedStatement.setString(8, paramCode2);
			oPreparedStatement.setString(9, paramValue2);
			oPreparedStatement.setString(10, paramCode3);
			oPreparedStatement.setString(11, paramValue3);
			oPreparedStatement.setString(12, paramCode4);
			oPreparedStatement.setString(13, paramValue4);
			oPreparedStatement.setString(14, paramCode5);
			oPreparedStatement.setString(15, paramValue5);
			oPreparedStatement.setString(16, paramCode6);
			oPreparedStatement.setString(17, paramValue6);
			oPreparedStatement.setString(18, paramCode7);
			oPreparedStatement.setString(19, paramValue7);
			oPreparedStatement.setString(20, paramCode8);
			oPreparedStatement.setString(21, paramValue8);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("CacheAuditRequest", "CacheAuditRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int rowsAffected = oPreparedStatement.executeUpdate();

			if (rowsAffected > 0) {

				rs = oPreparedStatement.getGeneratedKeys(); // Using generated key we are capturing auto generated id

				ObjectNode oCacheAuditObjNode = JsonNodeFactory.instance.objectNode();
				ObjectNode oCacheAuditDataObjNode = oCacheAuditObjNode.putObject("CacheAuditResponse");
				ObjectNode oCacheAuditDataSuccess = oCacheAuditDataObjNode.putObject("success");

				if (rs.next()) {
					Integer generatedId = rs.getInt(1); // retrieved value of the id

					oCacheAuditDataSuccess.put("ID", generatedId);
					oCacheAuditDataSuccess.put("PROCESS_CODE", processCode);
					oCacheAuditDataSuccess.put("PROCESS_DATA_REF", processDataRef);
				}

				if (oAppLogger != null) {
					oAppLogger.auditAPIResponse("CacheAuditResponse", "CacheAuditResponse", null,
							oCacheAuditObjNode.toString(), exchange);
				}

				return oCacheAuditObjNode;
			}

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}

		return null;

	}

	/**
	 * This api is meant to delete data from process data table
	 * 
	 * @param processCode
	 * @param cacheKey
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode DeleteCache(@Simple("${header.PROCESS_CODE}") Integer processCode,
			@Simple("${header.CACHE_KEY}") String cacheKey, Exchange exchange) throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {
			conn = cacheDataSource.getConnection();
			String query = "DELETE FROM DH_REPO_CACHE_PROCESS_DATA WHERE PROCESS_CODE= ? AND CACHE_KEY= ?";

			oPreparedStatement = conn.prepareStatement(query.toString());
			oPreparedStatement.setInt(1, processCode);
			oPreparedStatement.setString(2, cacheKey);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("CacheDeleteRequest", "CacheDeleteRequest",
						exchange.getIn().getHeader("PROCESS_CODE", Integer.class)
								+ exchange.getIn().getHeader("CACHE_KEY", String.class),
						null, exchange);
			}

			int rowDeleted = oPreparedStatement.executeUpdate();
			ObjectNode oDeleteCacheObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oDeleteCacheResponseObjNode = oDeleteCacheObjNode.putObject("CacheDeletionResponse");
			if (rowDeleted > 0) {
				oDeleteCacheResponseObjNode.put("CACHE_KEY", cacheKey);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("CacheDeletionResponse", "CacheDeletionResponse", null,
						oDeleteCacheObjNode.toString(), exchange);
			}

			return oDeleteCacheObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * This Api is used to update status of process data table according to the
	 * Process_Code and Cache_key
	 * 
	 * @param status
	 * @param processCode
	 * @param cacheKey
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode UpdateCache(@Simple("${body[CacheUpdationRequest][STATUS]}") String status,
			@Simple("${body[CacheUpdationRequest][PROCESS_CODE]}") Integer processCode,
			@Simple("${body[CacheUpdationRequest][CACHE_KEY]}") String cacheKey, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {
			conn = cacheDataSource.getConnection();
			String query = "UPDATE DH_REPO_CACHE_PROCESS_DATA SET STATUS= ? WHERE PROCESS_CODE= ? AND CACHE_KEY= ?";

			oPreparedStatement = conn.prepareStatement(query);
			oPreparedStatement.setString(1, status);
			oPreparedStatement.setInt(2, processCode);
			oPreparedStatement.setString(3, cacheKey);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("CacheUpdationRequest", "CacheUpdationRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int rowUpdated = oPreparedStatement.executeUpdate();

			ObjectNode oUpdateCacheObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oUpdateCacheResponseObjNode = oUpdateCacheObjNode.putObject("CacheUpdationResponse");
			if (rowUpdated > 0) {
				oUpdateCacheResponseObjNode.put("STATUS", status);
				oUpdateCacheResponseObjNode.put("CACHE_KEY", cacheKey);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("CacheUpdationResponse", "CacheUpdationResponse", null,
						oUpdateCacheObjNode.toString(), exchange);
			}

			return oUpdateCacheObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * This Api is used to Add data to the process data table
	 * 
	 * @param cacheKey
	 * @param processCode
	 * @param paramCode1
	 * @param paramValue1
	 * @param paramCode2
	 * @param paramValue2
	 * @param paramCode3
	 * @param paramValue3
	 * @param paramCode4
	 * @param paramValue4
	 * @param paramCode5
	 * @param paramValue5
	 * @param paramCode6
	 * @param paramValue6
	 * @param paramCode7
	 * @param paramValue7
	 * @param paramCode8
	 * @param paramValue8
	 * @param createdOn
	 * @param createdBy
	 * @param status
	 * @param responseObject
	 * @param errorCount
	 * @param responseType
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode AddCache(@Simple("${body[AddCacheRequest][CACHE_KEY]}") String cacheKey,
			@Simple("${body[AddCacheRequest][PROCESS_CODE]}") Integer processCode,
			@Simple("${body[AddCacheRequest][PARAM_CODE1]}") String paramCode1,
			@Simple("${body[AddCacheRequest][PARAM_VALUE1]}") String paramValue1,
			@Simple("${body[AddCacheRequest][PARAM_CODE2]}") String paramCode2,
			@Simple("${body[AddCacheRequest][PARAM_VALUE2]}") String paramValue2,
			@Simple("${body[AddCacheRequest][PARAM_CODE3]}") String paramCode3,
			@Simple("${body[AddCacheRequest][PARAM_VALUE3]}") String paramValue3,
			@Simple("${body[AddCacheRequest][PARAM_CODE4]}") String paramCode4,
			@Simple("${body[AddCacheRequest][PARAM_VALUE4]}") String paramValue4,
			@Simple("${body[AddCacheRequest][PARAM_CODE5]}") String paramCode5,
			@Simple("${body[AddCacheRequest][PARAM_VALUE5]}") String paramValue5,
			@Simple("${body[AddCacheRequest][PARAM_CODE6]}") String paramCode6,
			@Simple("${body[AddCacheRequest][PARAM_VALUE6]}") String paramValue6,
			@Simple("${body[AddCacheRequest][PARAM_CODE7]}") String paramCode7,
			@Simple("${body[AddCacheRequest][PARAM_VALUE7]}") String paramValue7,
			@Simple("${body[AddCacheRequest][PARAM_CODE8]}") String paramCode8,
			@Simple("${body[AddCacheRequest][PARAM_VALUE8]}") String paramValue8,
			@Simple("${body[AddCacheRequest][CREATED_ON]}") String createdOn,
			@Simple("${body[AddCacheRequest][CREATED_BY]}") String createdBy,
			@Simple("${body[AddCacheRequest][STATUS]}") String status,
			@Simple("${body[AddCacheRequest][RESPONSE_OBJECT]}") String responseObject,
			@Simple("${body[AddCacheRequest][ERROR_COUNT]}") Integer errorCount,
			@Simple("${body[AddCacheRequest][RESPONSE_TYPE]}") String responseType, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {
			conn = cacheDataSource.getConnection();
			String query = "INSERT INTO DH_REPO_CACHE_PROCESS_DATA(CACHE_KEY,PROCESS_CODE,PARAM_CODE1,PARAM_VALUE1,PARAM_CODE2,PARAM_VALUE2,PARAM_CODE3,PARAM_VALUE3,PARAM_CODE4,PARAM_VALUE4,PARAM_CODE5,PARAM_VALUE5,PARAM_CODE6,PARAM_VALUE6,PARAM_CODE7,PARAM_VALUE7,"
					+ "PARAM_CODE8,PARAM_VALUE8,CREATED_ON,CREATED_BY,STATUS,RESPONSE_OBJECT,ERROR_COUNT,RESPONSE_TYPE)"
					+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

			oPreparedStatement = conn.prepareStatement(query, new int[] { 1 }); // 1 indicates the 'ID' column
			oPreparedStatement.setString(1, cacheKey);
			oPreparedStatement.setInt(2, processCode);
			oPreparedStatement.setString(3, paramCode1);
			oPreparedStatement.setString(4, paramValue1);
			oPreparedStatement.setString(5, paramCode2);
			oPreparedStatement.setString(6, paramValue2);
			oPreparedStatement.setString(7, paramCode3);
			oPreparedStatement.setString(8, paramValue3);
			oPreparedStatement.setString(9, paramCode4);
			oPreparedStatement.setString(10, paramValue4);
			oPreparedStatement.setString(11, paramCode5);
			oPreparedStatement.setString(12, paramValue5);
			oPreparedStatement.setString(13, paramCode6);
			oPreparedStatement.setString(14, paramValue6);
			oPreparedStatement.setString(15, paramCode7);
			oPreparedStatement.setString(16, paramValue7);
			oPreparedStatement.setString(17, paramCode8);
			oPreparedStatement.setString(18, paramValue8);
			oPreparedStatement.setDate(19,
					java.sql.Date.valueOf(oDateUtils.convertToLocalDate(createdOn, "dd-MM-yyyy")));
			oPreparedStatement.setString(20, createdBy);
			oPreparedStatement.setString(21, status);
			oPreparedStatement.setString(22, responseObject);
			oPreparedStatement.setInt(23, errorCount);
			oPreparedStatement.setString(24, responseType);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("AddCacheRequest", "AddCacheRequest", exchange.getIn().getBody(String.class),
						null, exchange);
			}

			int rowInserted = oPreparedStatement.executeUpdate();

			ObjectNode oAddCacheObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oAddCacheResponseObjNode = oAddCacheObjNode.putObject("AddCacheResponse");
			if (rowInserted > 0) {
				rs = oPreparedStatement.getGeneratedKeys();
				if (rs.next()) {
					Integer generatedId = rs.getInt(1);
					oAddCacheResponseObjNode.put("ID", generatedId);
					oAddCacheResponseObjNode.put("CACHE_KEY", cacheKey);
				}
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("AddCacheResponse", "AddCacheResponse", null, oAddCacheObjNode.toString(),
						exchange);
			}

			return oAddCacheObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for getting project master data.
	 * 
	 * @param projectCode
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode GetProject(@Simple("${header.projectCode}") String projectCode, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("SELECT * FROM DH_REPO_CACHE_PROJECT_MASTER");
			if (null != projectCode)
				query.append(" WHERE PROJECT_CODE=:PROJECT_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			if (null != projectCode)
				oPreparedStatement.setString(1, projectCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProjectRequest", "ProjectRequest", exchange.getIn().getBody(String.class),
						null, exchange);
			}

			rs = oPreparedStatement.executeQuery();

			ResultSetMetaData rsMetadata = null;
			rsMetadata = rs.getMetaData();
			int noOfColumns = rsMetadata.getColumnCount();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProjectResponse");
			ArrayNode oProcessArryNode = oProcessResponseObjNode.putArray("project");
			while (rs.next()) {

				ObjectNode oProcessObjNode = JsonNodeFactory.instance.objectNode();
				for (int i = 1; i <= noOfColumns; i++) {

					String columnName = rsMetadata.getColumnName(i);
					String columnValue = rs.getString(i);
					oProcessObjNode.put(columnName, columnValue);
				}
				oProcessArryNode.add(oProcessObjNode);

			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProjectResponse", "ProjectResponse", null, oProcessParObjNode.toString(),
						exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for inserting in project master data.
	 * 
	 * @param projectCode
	 * @param projectDescription
	 * @param createdBy
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode AddProject(@Simple("${body[ProjectCreationRequest][projectCode]}") String projectCode,
			@Simple("${body[ProjectCreationRequest][projectDescription]}") String projectDescription,
			@Simple("${body[ProjectCreationRequest][createdBy]}") String createdBy, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append(
					"INSERT INTO DH_REPO_CACHE_PROJECT_MASTER(PROJECT_CODE,PROJECT_DESCRIPTION,CREATED_ON,CREATED_BY)"
							+ " VALUES (:PROJECT_CODE,:PROJECT_DESCRIPTION,:CREATED_ON,:CREATED_BY)");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setString(1, projectCode);
			oPreparedStatement.setString(2, projectDescription);
			oPreparedStatement.setDate(3, new Date(System.currentTimeMillis()));
			oPreparedStatement.setString(4, createdBy);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProjectCreationRequest", "ProjectCreationRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProjectCreationResponse");
			if (noOfRows > 0)
				oProcessResponseObjNode.put("projectCode", projectCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProjectCreationResponse", "ProjectCreationResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for updating in project master data.
	 * 
	 * @param projectCode
	 * @param projectDescription
	 * @param modifiedBy
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode UpdateProject(@Simple("${body[ProjectUpdationRequest][projectCode]}") String projectCode,
			@Simple("${body[ProjectUpdationRequest][projectDescription]}") String projectDescription,
			@Simple("${body[ProjectUpdationRequest][modifiedBy]}") String modifiedBy, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("UPDATE DH_REPO_CACHE_PROJECT_MASTER "
					+ "SET PROJECT_DESCRIPTION=:PROJECT_DESCRIPTION,MODIFIED_ON=:MODIFIED_ON,MODIFIED_BY=:MODIFIED_BY "
					+ "WHERE PROJECT_CODE=:PROJECT_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setString(1, projectDescription);
			oPreparedStatement.setDate(2, new Date(System.currentTimeMillis()));
			oPreparedStatement.setString(3, modifiedBy);
			oPreparedStatement.setString(4, projectCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProjectUpdationRequest", "ProjectUpdationRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProjectUpdationResponse");

			if (noOfRows > 0)
				oProcessResponseObjNode.put("projectCode", projectCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProjectUpdationResponse", "ProjectUpdationResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for deleting from project master data.
	 * 
	 * @param projectCode
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode DeleteProject(@Simple("${header.projectCode}") String projectCode, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("DELETE FROM DH_REPO_CACHE_PROJECT_MASTER WHERE PROJECT_CODE=:PROJECT_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setString(1, projectCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProjectDeletionRequest", "ProjectDeletionRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProjectDeletionResponse");

			if (noOfRows > 0)
				oProcessResponseObjNode.put("projectCode", projectCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProjectDeletionResponse", "ProjectDeletionResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for getting process master data.
	 * 
	 * @param projectCode
	 * @param processCode
	 * @param status
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode GetProcess(@Simple("${header.projectCode}") String projectCode,
			@Simple("${header.processCode}") String processCode, @Simple("${header.status}") String status,
			Exchange exchange) throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("SELECT * FROM DH_REPO_CACHE_PROCESS_MASTER");
			if (null != projectCode && null != processCode && null != status)
				query.append(" WHERE PROJECT_CODE=:PROJECT_CODE AND PROCESS_CODE=:PROCESS_CODE AND STATUS=:STATUS");
			else if (null != projectCode && null != processCode)
				query.append(" WHERE PROJECT_CODE=:PROJECT_CODE AND PROCESS_CODE=:PROCESS_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			if (null != projectCode && null != processCode && null != status) {
				oPreparedStatement.setString(1, projectCode);
				oPreparedStatement.setString(2, processCode);
				oPreparedStatement.setString(3, status);
			} else if (null != projectCode && null != processCode) {
				oPreparedStatement.setString(1, projectCode);
				oPreparedStatement.setString(2, processCode);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessRequest", "ProcessRequest", exchange.getIn().getBody(String.class),
						null, exchange);
			}

			rs = oPreparedStatement.executeQuery();

			ResultSetMetaData rsMetadata = null;
			rsMetadata = rs.getMetaData();
			int noOfColumns = rsMetadata.getColumnCount();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessResponse");
			ArrayNode oProcessArryNode = oProcessResponseObjNode.putArray("process");
			while (rs.next()) {

				ObjectNode oProcessObjNode = JsonNodeFactory.instance.objectNode();
				for (int i = 1; i <= noOfColumns; i++) {

					String columnName = rsMetadata.getColumnName(i);
					String columnValue = rs.getString(i);
					oProcessObjNode.put(columnName, columnValue);
				}
				oProcessArryNode.add(oProcessObjNode);

			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessResponse", "ProcessResponse", null, oProcessParObjNode.toString(),
						exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for inserting in process master data.
	 * 
	 * @param projectCode
	 * @param processCode
	 * @param processDescription
	 * @param cacheTime
	 * @param errorCount
	 * @param cacheErrorResponse
	 * @param additional1
	 * @param additional2
	 * @param additional3
	 * @param additional4
	 * @param additional5
	 * @param timeSpecificCache
	 * @param createdBy
	 * @param responseXpath
	 * @param cacheTimeUnit
	 * @param errorTime
	 * @param errorTimeUnit
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode AddProcess(@Simple("${body[ProcessCreationRequest][projectCode]}") String projectCode,
			@Simple("${body[ProcessCreationRequest][processCode]}") String processCode,
			@Simple("${body[ProcessCreationRequest][processDescription]}") String processDescription,
			@Simple("${body[ProcessCreationRequest][cacheTime]}") Integer cacheTime,
			@Simple("${body[ProcessCreationRequest][status]}") String status,
			@Simple("${body[ProcessCreationRequest][errorCount]}") String errorCount,
			@Simple("${body[ProcessCreationRequest][cacheErrorResponse]}") String cacheErrorResponse,
			@Simple("${body[ProcessCreationRequest][additional1]}") String additional1,
			@Simple("${body[ProcessCreationRequest][additional2]}") String additional2,
			@Simple("${body[ProcessCreationRequest][additional3]}") String additional3,
			@Simple("${body[ProcessCreationRequest][additional4]}") String additional4,
			@Simple("${body[ProcessCreationRequest][additional5]}") String additional5,
			@Simple("${body[ProcessCreationRequest][timeSpecificCache]}") String timeSpecificCache,
			@Simple("${body[ProcessCreationRequest][createdBy]}") String createdBy,
			@Simple("${body[ProcessCreationRequest][responseXpath]}") String responseXpath,
			@Simple("${body[ProcessCreationRequest][cacheTimeUnit]}") String cacheTimeUnit,
			@Simple("${body[ProcessCreationRequest][errorTime]}") Integer errorTime,
			@Simple("${body[ProcessCreationRequest][errorTimeUnit]}") String errorTimeUnit, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append(
					"INSERT INTO DH_REPO_CACHE_PROCESS_MASTER(PROJECT_CODE,PROCESS_CODE,PROCESS_DESCRIPTION,CACHE_TIME,STATUS,ERROR_COUNT,CACHE_ERROR_RESPONSE, "
							+ "ADDITIONAL1,ADDITIONAL2,ADDITIONAL3,ADDITIONAL4,ADDITIONAL5,TIME_SPECIFIC_CACHE,CREATED_ON,CREATED_BY,RESPONSE_XPATH,CACHE_TIME_UNIT,ERROR_TIME,ERROR_TIME_UNIT) "
							+ "VALUES(:PROJECT_CODE,:PROCESS_CODE,:PROCESS_DESCRIPTION,:CACHE_TIME,:STATUS,:ERROR_COUNT,:CACHE_ERROR_RESPONSE,:ADDITIONAL1,:ADDITIONAL2,"
							+ ":ADDITIONAL3,:ADDITIONAL4,:ADDITIONAL5,:TIME_SPECIFIC_CACHE,:CREATED_ON,:CREATED_BY,:RESPONSE_XPATH,:CACHE_TIME_UNIT,:ERROR_TIME,:ERROR_TIME_UNIT)");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setString(1, projectCode);
			oPreparedStatement.setString(2, processCode);
			oPreparedStatement.setString(3, processDescription);
			oPreparedStatement.setInt(4, cacheTime);
			oPreparedStatement.setString(5, status);
			oPreparedStatement.setString(6, errorCount);
			oPreparedStatement.setString(7, cacheErrorResponse);
			oPreparedStatement.setString(8, additional1);
			oPreparedStatement.setString(9, additional2);
			oPreparedStatement.setString(10, additional3);
			oPreparedStatement.setString(11, additional4);
			oPreparedStatement.setString(12, additional5);
			oPreparedStatement.setString(13, timeSpecificCache);
			oPreparedStatement.setDate(14, new Date(System.currentTimeMillis()));
			oPreparedStatement.setString(15, createdBy);
			oPreparedStatement.setString(16, responseXpath);
			oPreparedStatement.setString(17, cacheTimeUnit);
			oPreparedStatement.setInt(18, errorTime);
			oPreparedStatement.setString(19, errorTimeUnit);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessCreationRequest", "ProcessCreationRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessCreationResponse");

			if (noOfRows > 0) {
				oProcessResponseObjNode.put("projectCode", projectCode);
				oProcessResponseObjNode.put("processCode", processCode);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessCreationResponse", "ProcessCreationResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for updating in process master data.
	 * 
	 * @param projectCode
	 * @param processCode
	 * @param processDescription
	 * @param cacheTime
	 * @param errorCount
	 * @param cacheErrorResponse
	 * @param additional1
	 * @param additional2
	 * @param additional3
	 * @param additional4
	 * @param additional5
	 * @param timeSpecificCache
	 * @param createdBy
	 * @param responseXpath
	 * @param cacheTimeUnit
	 * @param errorTime
	 * @param errorTimeUnit
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode UpdateProcess(@Simple("${body[ProcessUpdationRequest][projectCode]}") String projectCode,
			@Simple("${body[ProcessUpdationRequest][processCode]}") String processCode,
			@Simple("${body[ProcessUpdationRequest][processDescription]}") String processDescription,
			@Simple("${body[ProcessUpdationRequest][cacheTime]}") Integer cacheTime,
			@Simple("${body[ProcessUpdationRequest][status]}") String status,
			@Simple("${body[ProcessUpdationRequest][errorCount]}") String errorCount,
			@Simple("${body[ProcessUpdationRequest][cacheErrorResponse]}") String cacheErrorResponse,
			@Simple("${body[ProcessUpdationRequest][additional1]}") String additional1,
			@Simple("${body[ProcessUpdationRequest][additional2]}") String additional2,
			@Simple("${body[ProcessUpdationRequest][additional3]}") String additional3,
			@Simple("${body[ProcessUpdationRequest][additional4]}") String additional4,
			@Simple("${body[ProcessUpdationRequest][additional5]}") String additional5,
			@Simple("${body[ProcessUpdationRequest][timeSpecificCache]}") String timeSpecificCache,
			@Simple("${body[ProcessUpdationRequest][modifiedBy]}") String modifiedBy,
			@Simple("${body[ProcessUpdationRequest][responseXpath]}") String responseXpath,
			@Simple("${body[ProcessUpdationRequest][cacheTimeUnit]}") String cacheTimeUnit,
			@Simple("${body[ProcessUpdationRequest][errorTime]}") Integer errorTime,
			@Simple("${body[ProcessUpdationRequest][errorTimeUnit]}") String errorTimeUnit, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("UPDATE DH_REPO_CACHE_PROCESS_MASTER\n"
					+ "SET PROJECT_CODE=:PROJECT_CODE,PROCESS_CODE=:PROCESS_CODE,PROCESS_DESCRIPTION=:PROCESS_DESCRIPTION,CACHE_TIME=:CACHE_TIME,STATUS=:STATUS,ERROR_COUNT=:ERROR_COUNT,CACHE_ERROR_RESPONSE=:CACHE_ERROR_RESPONSE,ADDITIONAL1=:ADDITIONAL1,ADDITIONAL2=:ADDITIONAL2,ADDITIONAL3=:ADDITIONAL3,"
					+ "ADDITIONAL4=:ADDITIONAL4,ADDITIONAL5=:ADDITIONAL5,TIME_SPECIFIC_CACHE=:TIME_SPECIFIC_CACHE,MODIFIED_ON=:MODIFIED_ON,MODIFIED_BY=:MODIFIED_BY,RESPONSE_XPATH=:RESPONSE_XPATH,CACHE_TIME_UNIT=:CACHE_TIME_UNIT,ERROR_TIME=:ERROR_TIME,ERROR_TIME_UNIT=:ERROR_TIME_UNIT "
					+ "WHERE  PROJECT_CODE=:PROJECT_CODE AND PROCESS_CODE=:PROCESS_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setString(1, projectCode);
			oPreparedStatement.setString(2, processCode);
			oPreparedStatement.setString(3, processDescription);
			oPreparedStatement.setInt(4, cacheTime);
			oPreparedStatement.setString(5, status);
			oPreparedStatement.setString(6, errorCount);
			oPreparedStatement.setString(7, cacheErrorResponse);
			oPreparedStatement.setString(8, additional1);
			oPreparedStatement.setString(9, additional2);
			oPreparedStatement.setString(10, additional3);
			oPreparedStatement.setString(11, additional4);
			oPreparedStatement.setString(12, additional5);
			oPreparedStatement.setString(13, timeSpecificCache);
			oPreparedStatement.setDate(14, new Date(System.currentTimeMillis()));
			oPreparedStatement.setString(15, modifiedBy);
			oPreparedStatement.setString(16, responseXpath);
			oPreparedStatement.setString(17, cacheTimeUnit);
			oPreparedStatement.setInt(18, errorTime);
			oPreparedStatement.setString(19, errorTimeUnit);
			oPreparedStatement.setString(20, projectCode);
			oPreparedStatement.setString(21, processCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessUpdationRequest", "ProcessUpdationRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessUpdationResponse");

			if (noOfRows > 0) {
				oProcessResponseObjNode.put("projectCode", projectCode);
				oProcessResponseObjNode.put("processCode", processCode);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessUpdationResponse", "ProcessUpdationResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for deleting from process master data.
	 * 
	 * @param projectCode
	 * @param processCode
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode DeleteProcess(@Simple("${header.projectCode}") String projectCode,
			@Simple("${header.processCode}") String processCode, Exchange exchange) throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append(
					"DELETE FROM DH_REPO_CACHE_PROCESS_MASTER WHERE PROJECT_CODE=:PROJECT_CODE AND PROCESS_CODE=:PROCESS_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setString(1, projectCode);
			oPreparedStatement.setString(2, processCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessDeletionRequest", "ProcessDeletionRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessDeletionResponse");

			if (noOfRows > 0) {
				oProcessResponseObjNode.put("projectCode", projectCode);
				oProcessResponseObjNode.put("processCode", processCode);
			}
			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessDeletionResponse", "ProcessDeletionResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for getting process params master data.
	 * 
	 * @param processId
	 * @param paramCode
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode GetProcessParams(@Simple("${header.processId}") Integer processId,
			@Simple("${header.paramCode}") String paramCode, Exchange exchange) throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;
		ResultSet rs = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append("SELECT PROCESS_CODE AS PROCESS_ID,PARAM_CODE,PARAM_XPATH,PARAM_TYPE,PARAM_FORMAT,PARAM_ORDER,"
					+ "IS_CACHE_VARIABLE,IS_LOGGED,ADDITIONAL1,ADDITIONAL2,ADDITIONAL3,ADDITIONAL4,ADDITIONAL5,"
					+ "IS_ERROR_VARIABLE,CREATED_ON,CREATED_BY FROM DH_REPO_CACHE_PROCESS_PARAMS");
			if (null != processId && null != paramCode)
				query.append(" WHERE PROCESS_CODE=:PROCESS_ID AND PARAM_CODE=:PARAM_CODE");
			else if (null != processId)
				query.append(" WHERE PROCESS_CODE=:PROCESS_ID");

			oPreparedStatement = conn.prepareStatement(query.toString());

			if (null != processId && null != paramCode) {
				oPreparedStatement.setInt(1, processId);
				oPreparedStatement.setString(2, paramCode);
			} else if (null != processId)
				oPreparedStatement.setInt(1, processId);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessParamsRequest", "ProcessParamsRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			rs = oPreparedStatement.executeQuery();

			ResultSetMetaData rsMetadata = null;
			rsMetadata = rs.getMetaData();
			int noOfColumns = rsMetadata.getColumnCount();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessParamsResponse");
			ArrayNode oProcessArryNode = oProcessResponseObjNode.putArray("processParam");
			while (rs.next()) {

				ObjectNode oProcessObjNode = JsonNodeFactory.instance.objectNode();
				for (int i = 1; i <= noOfColumns; i++) {

					String columnName = rsMetadata.getColumnName(i);
					String columnValue = rs.getString(i);
					oProcessObjNode.put(columnName, columnValue);
				}
				oProcessArryNode.add(oProcessObjNode);

			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessParamsResponse", "ProcessParamsResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != rs)
				rs.close();
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for inserting in process params master data.
	 * 
	 * @param processCode
	 * @param paramCode
	 * @param paramXpath
	 * @param paramType
	 * @param paramFormat
	 * @param paramOrder
	 * @param isCacheVariable
	 * @param isLogged
	 * @param additional1
	 * @param additional2
	 * @param additional3
	 * @param additional4
	 * @param additional5
	 * @param isErrorVariable
	 * @param createdBy
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode AddProcessParams(@Simple("${body[ProcessParamsCreationRequest][processId]}") Integer processId,
			@Simple("${body[ProcessParamsCreationRequest][paramCode]}") String paramCode,
			@Simple("${body[ProcessParamsCreationRequest][paramXpath]}") String paramXpath,
			@Simple("${body[ProcessParamsCreationRequest][paramType]}") String paramType,
			@Simple("${body[ProcessParamsCreationRequest][paramFormat]}") String paramFormat,
			@Simple("${body[ProcessParamsCreationRequest][paramOrder]}") Integer paramOrder,
			@Simple("${body[ProcessParamsCreationRequest][isCacheVariable]}") String isCacheVariable,
			@Simple("${body[ProcessParamsCreationRequest][isLogged]}") String isLogged,
			@Simple("${body[ProcessParamsCreationRequest][additional1]}") String additional1,
			@Simple("${body[ProcessParamsCreationRequest][additional2]}") String additional2,
			@Simple("${body[ProcessParamsCreationRequest][additional3]}") String additional3,
			@Simple("${body[ProcessParamsCreationRequest][additional4]}") String additional4,
			@Simple("${body[ProcessParamsCreationRequest][additional5]}") String additional5,
			@Simple("${body[ProcessParamsCreationRequest][isErrorVariable]}") String isErrorVariable,
			@Simple("${body[ProcessParamsCreationRequest][createdBy]}") String createdBy, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append(
					"INSERT INTO DH_REPO_CACHE_PROCESS_PARAMS(PROCESS_CODE,PARAM_CODE,PARAM_XPATH,PARAM_TYPE,PARAM_FORMAT,PARAM_ORDER,"
							+ "IS_CACHE_VARIABLE,IS_LOGGED,ADDITIONAL1,ADDITIONAL2,ADDITIONAL3,ADDITIONAL4,ADDITIONAL5,IS_ERROR_VARIABLE,CREATED_BY,CREATED_ON)"
							+ " VALUES(:PROCESS_CODE,:PARAM_CODE,:PARAM_XPATH,:PARAM_TYPE,:PARAM_FORMAT,:PARAM_ORDER,:IS_CACHE_VARIABLE,:IS_LOGGED,"
							+ ":ADDITIONAL1,:ADDITIONAL2,:ADDITIONAL3,:ADDITIONAL4,:ADDITIONAL5,:IS_ERROR_VARIABLE,:CREATED_BY,:CREATED_ON)");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setInt(1, processId);
			oPreparedStatement.setString(2, paramCode);
			oPreparedStatement.setString(3, paramXpath);
			oPreparedStatement.setString(4, paramType);
			oPreparedStatement.setString(5, paramFormat);
			oPreparedStatement.setInt(6, paramOrder);
			oPreparedStatement.setString(7, isCacheVariable);
			oPreparedStatement.setString(8, isLogged);
			oPreparedStatement.setString(9, additional1);
			oPreparedStatement.setString(10, additional2);
			oPreparedStatement.setString(11, additional3);
			oPreparedStatement.setString(12, additional4);
			oPreparedStatement.setString(13, additional5);
			oPreparedStatement.setString(14, isErrorVariable);
			oPreparedStatement.setString(15, createdBy);
			oPreparedStatement.setDate(16, new Date(System.currentTimeMillis()));

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessParamsCreationRequest", "ProcessParamsCreationRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessParamsCreationResponse");

			if (noOfRows > 0) {
				oProcessResponseObjNode.put("processId", processId);
				oProcessResponseObjNode.put("paramCode", paramCode);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessParamsCreationResponse", "ProcessParamsCreationResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for updating in process params master data.
	 * 
	 * @param processCode
	 * @param paramCode
	 * @param paramXpath
	 * @param paramType
	 * @param paramFormat
	 * @param paramOrder
	 * @param isCacheVariable
	 * @param isLogged
	 * @param additional1
	 * @param additional2
	 * @param additional3
	 * @param additional4
	 * @param additional5
	 * @param isErrorVariable
	 * @param modifiedBy
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode UpdateProcessParams(@Simple("${body[ProcessParamsUpdationRequest][processId]}") Integer processId,
			@Simple("${body[ProcessParamsUpdationRequest][paramCode]}") String paramCode,
			@Simple("${body[ProcessParamsUpdationRequest][paramXpath]}") String paramXpath,
			@Simple("${body[ProcessParamsUpdationRequest][paramType]}") String paramType,
			@Simple("${body[ProcessParamsUpdationRequest][paramFormat]}") String paramFormat,
			@Simple("${body[ProcessParamsUpdationRequest][paramOrder]}") Integer paramOrder,
			@Simple("${body[ProcessParamsUpdationRequest][isCacheVariable]}") String isCacheVariable,
			@Simple("${body[ProcessParamsUpdationRequest][isLogged]}") String isLogged,
			@Simple("${body[ProcessParamsUpdationRequest][additional1]}") String additional1,
			@Simple("${body[ProcessParamsUpdationRequest][additional2]}") String additional2,
			@Simple("${body[ProcessParamsUpdationRequest][additional3]}") String additional3,
			@Simple("${body[ProcessParamsUpdationRequest][additional4]}") String additional4,
			@Simple("${body[ProcessParamsUpdationRequest][additional5]}") String additional5,
			@Simple("${body[ProcessParamsUpdationRequest][isErrorVariable]}") String isErrorVariable,
			@Simple("${body[ProcessParamsUpdationRequest][modifiedBy]}") String modifiedBy, Exchange exchange)
			throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append(
					"UPDATE DH_REPO_CACHE_PROCESS_PARAMS SET PROCESS_CODE=:PROCESS_CODE,PARAM_CODE=:PARAM_CODE,PARAM_XPATH=:PARAM_XPATH,PARAM_TYPE=:PARAM_TYPE,PARAM_FORMAT=:PARAM_FORMAT,PARAM_ORDER=:PARAM_ORDER,IS_CACHE_VARIABLE=:IS_CACHE_VARIABLE,"
							+ "IS_LOGGED=:IS_LOGGED,ADDITIONAL1=:ADDITIONAL1,ADDITIONAL2=:ADDITIONAL2,ADDITIONAL3=:ADDITIONAL3,ADDITIONAL4=:ADDITIONAL4,ADDITIONAL5=:ADDITIONAL5,IS_ERROR_VARIABLE=:IS_ERROR_VARIABLE,MODIFIED_BY=:MODIFIED_BY,MODIFIED_ON=:MODIFIED_ON"
							+ " WHERE PROCESS_CODE=:PROCESS_CODE AND PARAM_CODE=:PARAM_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setInt(1, processId);
			oPreparedStatement.setString(2, paramCode);
			oPreparedStatement.setString(3, paramXpath);
			oPreparedStatement.setString(4, paramType);
			oPreparedStatement.setString(5, paramFormat);
			oPreparedStatement.setInt(6, paramOrder);
			oPreparedStatement.setString(7, isCacheVariable);
			oPreparedStatement.setString(8, isLogged);
			oPreparedStatement.setString(9, additional1);
			oPreparedStatement.setString(10, additional2);
			oPreparedStatement.setString(11, additional3);
			oPreparedStatement.setString(12, additional4);
			oPreparedStatement.setString(13, additional5);
			oPreparedStatement.setString(14, isErrorVariable);
			oPreparedStatement.setString(15, modifiedBy);
			oPreparedStatement.setDate(16, new Date(System.currentTimeMillis()));
			oPreparedStatement.setInt(17, processId);
			oPreparedStatement.setString(18, paramCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessParamsUpdationRequest", "ProcessParamsUpdationRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessParamsUpdationResponse");

			if (noOfRows > 0) {
				oProcessResponseObjNode.put("processId", processId);
				oProcessResponseObjNode.put("paramCode", paramCode);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessParamsUpdationResponse", "ProcessParamsUpdationResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

	/**
	 * this api is meant for deleting from process params master data.
	 * 
	 * @param processCode
	 * @param paramCode
	 * @param exchange
	 * @return
	 * @throws SQLException
	 */
	public JsonNode DeleteProcessParams(@Simple("${header.processId}") Integer processId,
			@Simple("${header.paramCode}") String paramCode, Exchange exchange) throws SQLException {

		Connection conn = null;
		PreparedStatement oPreparedStatement = null;

		try {
			conn = cacheDataSource.getConnection();
			StringBuffer query = new StringBuffer();
			query.append(
					"DELETE FROM DH_REPO_CACHE_PROCESS_PARAMS WHERE PROCESS_CODE=:PROCESS_CODE AND PARAM_CODE=:PARAM_CODE");

			oPreparedStatement = conn.prepareStatement(query.toString());

			oPreparedStatement.setInt(1, processId);
			oPreparedStatement.setString(2, paramCode);

			if (oAppLogger != null) {
				oAppLogger.auditAPIRequest("ProcessParamsDeletionRequest", "ProcessParamsDeletionRequest",
						exchange.getIn().getBody(String.class), null, exchange);
			}

			int noOfRows = oPreparedStatement.executeUpdate();

			ObjectNode oProcessParObjNode = JsonNodeFactory.instance.objectNode();
			ObjectNode oProcessResponseObjNode = oProcessParObjNode.putObject("ProcessParamsDeletionResponse");

			if (noOfRows > 0) {
				oProcessResponseObjNode.put("processId", processId);
				oProcessResponseObjNode.put("paramCode", paramCode);
			}

			if (oAppLogger != null) {
				oAppLogger.auditAPIResponse("ProcessParamsDeletionResponse", "ProcessParamsDeletionResponse", null,
						oProcessParObjNode.toString(), exchange);
			}

			return oProcessParObjNode;

		} catch (Exception oException) {

			cachedbLogger.error(oException.getMessage());

		} finally {
			if (null != oPreparedStatement)
				oPreparedStatement.close();
			if (conn != null)
				conn.close();

		}
		return null;

	}

}
