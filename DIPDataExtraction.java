package choiceportal.batch.dip;
import java.util.*;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import choiceportal.dbdriver.dip.*;
import choiceportal.dbdriver.*;


/**
 * @version 1.0
 * @author James J. Telecsan
 * @deprecated
 * 
 * This class is no longer used in processing DIPR (Delinquent Invoice Processing) data.
 * 
 * The DIPR processing system went through a rewrite in 2005-06 that changed the way this data is created.
 * DIPR Reports are still viewed and emailed using the ChoicePortal, but the source data is collected by the
 * DIPR system and placed in a flat file. That file is then collected and loaded into an Oracle database.
 * 
 * This class was orignially used to collect the DIPR data by running it's own queries.
 * 
 * <PRE>
 * REVISION HISTORY
 * ----------------
 * JJT  2004-02-10  Initial Version
 * </PRE>
 */
public class DIPDataExtraction {
	int iRunKey;
	DBConnectionManagerBatch objManager;
	
	/**
	 * 
	 */
	public DIPDataExtraction() {
		super();
	}

	private void executeReport(String argstrUserID, String argstrPass, String argstrCutoffDate) {
		
		// Cutoff date must be YYYY-MM-DD if supplied.
		System.out.println("Connecting to Databases          : " + new java.util.Date(System.currentTimeMillis()));
		objManager = new DBConnectionManagerBatch();
		if (this.getPlatform() == DBConnectionManager.PLATFORM_MAINFRAME) {
			if (!objManager.connectDB2(this.getPlatform(), this.getEnvDB2())) {
				System.out.println("Error Connecting to DB2!");
				System.out.println(objManager.getExceptionData());
				return;
			}
		} else {
			if (!objManager.connectDB2(this.getPlatform(), this.getEnvDB2(), argstrUserID, argstrPass)) {
				System.out.println("Error Connecting to DB2!");
				System.out.println(objManager.getExceptionData());
				return;
			}
		}
		
		if (!objManager.connectORA(this.getEnvOracle())) {
			System.out.println("Error Connecting to Oracle!");
			System.out.println(objManager.getExceptionData());
			objManager.disconnectDB2();
			return;
		}
		
		String strCutoffDate = argstrCutoffDate;
		boolean boolSuccess = true;
		if (strCutoffDate.length() != 10) {
			Calendar calCutoff = Calendar.getInstance();
			calCutoff.add(Calendar.DAY_OF_MONTH, -5);
			iRunKey = setRunKey(objManager.getConnectionORA(), calCutoff);
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			strCutoffDate = formatter.format(calCutoff.getTime());
		}
		System.out.println("Cutoff Date                      : " + strCutoffDate);
		//set Run Key here
		
		if (iRunKey != -1) {
			System.out.println("Run Key                          : " + iRunKey);
			
			if (boolSuccess) {boolSuccess = getDetailedInvoices(strCutoffDate);} 
			if (boolSuccess) {boolSuccess = buildWeeklySummary(); }

		}
		System.out.println("Disconnecting from Databases     : " + new java.util.Date(System.currentTimeMillis()));
		objManager.disconnectDB2();
		objManager.disconnectORA();
	}
	
	
	private void displayStatus(int total, int select, int insert) {
		if (total % 100 == 0) {
			if (total % 500 == 0) {
				if (total % 1000 == 0) {
					if (total % 5000 == 0) {
						System.out.println("| Total\t: " + total + "Insert\t: " + select + "Summary\t: " + insert + "Time\t:" + new java.util.Date(System.currentTimeMillis()));								
					} else {
						System.out.print("|");
					}
				} else {
					System.out.print("*");
				}
			} else {
				System.out.print("-");
			}
		}
	}
	
	private int setRunKey(Connection dbOracleConn, Calendar argCalendar) {
		TableDIPReportCatalog tableCatalog = new TableDIPReportCatalog(dbOracleConn, this.getEnvOracle());
		String strLastRun = tableCatalog.getMaxRunKey();
		int iLastRun = 0;
		if ((strLastRun != null) && (strLastRun.length() > 0)) {
			iLastRun = Integer.parseInt(strLastRun);
		}

		String strSQL = "INSERT INTO ERCOTDB.DIP_REPORT_CATALOG VALUES(?, ?)";
		try {
			PreparedStatement stmt = dbOracleConn.prepareStatement(strSQL);
			stmt.setInt(1, iLastRun+1);
			stmt.setDate(2, new java.sql.Date(argCalendar.getTime().getTime()));
			int i = stmt.executeUpdate();
			if (i != 1) {
				System.out.println("No REPORT CATALOG Row inserted");
			}
			stmt.close();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
			return -1;
		}
	
		return iLastRun+1;
	}

	private boolean buildWeeklySummary() {
		Connection connDB2 = objManager.getConnectionDB2();
		Connection connOracle = objManager.getConnectionORA();
		boolean boolReturn = true;
		String strRegion = "";
		if (this.getEnvDB2() == DBConnectionManagerBatch.ENV_PROD) {
			strRegion = "SPRODGRP";
		} else {
			strRegion = "SDEVLGRP";
		}

		String strTotalBilledSQL = 
			"SELECT COMPANY_CD, PROVIDER_ID, SUM(TOTAL_AMOUNT)" +
			"  FROM " + strRegion + ".DPR_INVOICE_SUMMRY" +
			" WHERE ACCOUNTING_DT >= '2002-01-01' " +
			"   AND ACCOUNTING_DT <= CURRENT DATE " +
			" GROUP BY COMPANY_CD, PROVIDER_ID ";
		
		String strLastPayDateSQL = 
			"SELECT A.COMPANY_CD, A.PROVIDER_ID, A.PAYMENT_DT_RECVD " +
			"  FROM " + strRegion + ".DPR_PAYMENT_SUMMRY A " +
			" WHERE A.STATUS_CD = 'IE' " +
			"   AND A.RECORD_SOURCE IN ('V','W') " +
			"   AND A.EFFECTIVE_DATETIME = " +
			"  (SELECT MAX(B.EFFECTIVE_DATETIME)" +
			"     FROM " + strRegion + ".DPR_PAYMENT_SUMMRY B " +
			"    WHERE B.COMPANY_CD = A.COMPANY_CD " +
			"      AND A.PROVIDER_ID = B.PROVIDER_ID " +
			"      AND B.STATUS_CD = 'IE' " +
			"      AND B.RECORD_SOURCE IN ('V','W'))" +
			" ORDER BY COMPANY_CD, PROVIDER_ID";
		
		String strWeeklyInsertSQL = 
			"INSERT INTO ERCOTDB.DIP_WEEKLY_SUMMARY VALUES " +
			"(?, ?, ?, ?, ?)";
			
		try {
			System.out.println("Getting Total Invoices to date   : " + new java.util.Date(System.currentTimeMillis()));
			Statement stmt = connDB2.createStatement();
			ResultSet rslt = stmt.executeQuery(strTotalBilledSQL);
			System.out.println("Processing Total Invoices to date: " + new java.util.Date(System.currentTimeMillis()));
			Map mapCompany = new HashMap();
			
			while (rslt.next()) {
				String strCompany = rslt.getString(1);
				String strProviderID = rslt.getString(2);
				Double dTotalBillAmount = new Double(rslt.getDouble(3));
				
				if (!mapCompany.containsKey(strCompany)) {
					mapCompany.put(strCompany, new HashMap());
				}
				
				Map mapProvider = (Map) mapCompany.get(strCompany);
				
				mapProvider.put(strProviderID, dTotalBillAmount);
			}
			
			System.out.println("Getting Last Pay Date            : " + new java.util.Date(System.currentTimeMillis()));
			rslt = stmt.executeQuery(strLastPayDateSQL);
			PreparedStatement pstmt = connOracle.prepareStatement(strWeeklyInsertSQL);
			System.out.println("Processing Last Pay Date         : " + new java.util.Date(System.currentTimeMillis()));
			while (rslt.next()) {
				String strCompany = rslt.getString(1);
				String strProviderID = rslt.getString(2);
				java.sql.Date dateLastPay = rslt.getDate(3);
				
				if (mapCompany.containsKey(strCompany)) {
					Map mapProvider = (Map) mapCompany.get(strCompany);
					if (mapProvider.containsKey(strProviderID)) {
						RowDIPWeeklySummary objRow = new RowDIPWeeklySummary();
						objRow.setRunKey(iRunKey);
						objRow.setCompanyCd(strCompany);
						objRow.setProviderID(strProviderID);
						objRow.setTotalBillAmt(((Double) mapProvider.get(strProviderID)).doubleValue());
						objRow.setLastPayDate(dateLastPay);
						this.insert(objRow, pstmt);
						mapProvider.remove(strProviderID);
						if (mapProvider.isEmpty()) {
							mapCompany.remove(strCompany);
						}
					} else {
						RowDIPWeeklySummary objRow = new RowDIPWeeklySummary();
						objRow.setRunKey(iRunKey);
						objRow.setCompanyCd(strCompany);
						objRow.setProviderID(strProviderID);
						objRow.setTotalBillAmt(0);
						objRow.setLastPayDate(dateLastPay);
						this.insert(objRow, pstmt);
					}
				} else {
					RowDIPWeeklySummary objRow = new RowDIPWeeklySummary();
					objRow.setRunKey(iRunKey);
					objRow.setCompanyCd(strCompany);
					objRow.setProviderID(strProviderID);
					objRow.setTotalBillAmt(0);
					objRow.setLastPayDate(dateLastPay);
					this.insert(objRow, pstmt);
				}
			}
			
			if (!mapCompany.isEmpty()) {
				Set setCompanies = mapCompany.entrySet();
				Iterator iterCompanies = setCompanies.iterator();
				while (iterCompanies.hasNext()) {
					Map.Entry entryCompany = (Map.Entry) iterCompanies.next();
					Map mapProviders = (Map) entryCompany.getValue();
					if (!mapProviders.isEmpty()) {
						Set setProviders = mapProviders.entrySet();
						Iterator iterProviders = setProviders.iterator();
						while (iterProviders.hasNext()) {
							Map.Entry entryProvider = (Map.Entry) iterProviders.next();
							RowDIPWeeklySummary objRow = new RowDIPWeeklySummary();
							objRow.setRunKey(iRunKey);
							objRow.setCompanyCd((String) entryCompany.getKey());
							objRow.setProviderID((String) entryProvider.getKey());
							objRow.setTotalBillAmt(((Double) entryProvider.getValue()).doubleValue());
							objRow.setLastPayDate(new java.sql.Date(1,1,1));
							insert(objRow, pstmt);
						}
					}
				}
			}
			
			stmt.close();
			pstmt.close();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
			boolReturn = false;
		}
		return boolReturn;
		
	}

	private boolean getDetailedInvoices(String argstrCutoffDate) {
		Connection connDB2 = objManager.getConnectionDB2();
		Connection connOracle = objManager.getConnectionORA();
		boolean boolReturn = true;
		
		Vector vectResult = new Vector();
		String strRegion = "";
		if (this.getEnvDB2() == DBConnectionManagerBatch.ENV_PROD) {
			strRegion = "SPRODGRP";
		} else {
			strRegion = "SDEVLGRP";
		}

		String strDetailSQL = 
		"SELECT A.COMPANY_CD, A.PROVIDER_ID, A.INVOICE_NBR " +
		"     ,	A.CANCEL_INVOICE_NBR, A.SERV_DELIV_ID, A.CROSS_REF_NBR " +
		"     , A.INVOICE_AMOUNT, A.TOTAL_PAID, A.PAYMENT_DUE_DT " +
		"     , A.ITEM_ID, A.STATUS_CD " +
		"     , (DAYS(CURRENT DATE) - DAYS(A.PAYMENT_DUE_DT) - 1) AS DAYS_LATE " +
		"     , A.LPC_FL, A.LPC_DT, A.REJECT_STATUS, A.REJECT_REASON_CD " +
		"     , A.REJECT_DT " +
		"  FROM " + strRegion + ".DPR_INVOICE_DETAIL A " +
		" WHERE A.STATUS_CD IN ('IE','CA','CR','DR','CD','DI','M$','C$','MH','R$','D$','MG') " +
		"   AND A.ISSUE_CD = ' ' " +
//		"   AND A.INVOICE_NBR = '94X4040303004907468243' " +
		"   AND A.EFFECTIVE_DATETIME = (SELECT MAX(B.EFFECTIVE_DATETIME)" +
		"       FROM " + strRegion + ".DPR_INVOICE_DETAIL B " +
		"      WHERE B.COMPANY_CD = A.COMPANY_CD " +
		"        AND B.PROVIDER_ID = A.PROVIDER_ID " +
		"        AND B.INVOICE_NBR = A.INVOICE_NBR " +
		"        AND B.CANCEL_INVOICE_NBR = A.CANCEL_INVOICE_NBR) ";
				
//		String strTCSQL = 
//		"SELECT TC_AMOUNT, TC_DISCOUNT_AMOUNT, DEPOSIT_APPLIED " +
//		"  FROM " + strRegion + ".DPR_TRANSITION_CHG B " +
//		" WHERE COMPANY_CD = '211' " +
//		"   AND INVOICE_NBR = ?";
		
		String strTCSQL = 
		"SELECT CHARGE_AT, DISCOUNT_AT, DEPOSIT_APPLIED_FL " +
		"  FROM " + strRegion + ".DPR_RIDER_CHARGE B " +
		" WHERE COMPANY_CD = '211' " +
		"   AND INVOICE_NBR = ? " +
		"  AND RIDER_TYPE_CD = 'TC'";
		
		String strInsertSQL = "INSERT INTO ERCOTDB.DIP_INVOICE_DETAIL VALUES ( " +
			"SEQ_DIP_INVOICE_DETAIL.NEXTVAL, " +
			"?, ?, ?, ?, ?, " +
			"?, ?, ?, ?, ?, " +
			"?, ?, ?, ?, ?, " +
			"?, ?, ?, ?, ?, " +
			"?, ?, ?, ?, ?) ";

		String strSummarySQL = "INSERT INTO ERCOTDB.DIP_CURRENT_INVOICE_SUMMARY VALUES ( " +
			"?, ?, ?, ?, ?," +
			"?, ?, ?, ?, ?," +
			"?, ?, ?, ?, ?)";
		
		Statement stmt = null;
		PreparedStatement pstmt = null;
		PreparedStatement istmt = null;
		
		try {
			stmt = connDB2.createStatement();
			pstmt = connDB2.prepareStatement(strTCSQL);
			istmt = connOracle.prepareStatement(strInsertSQL);
			
			System.out.println("Beginning Query Execution        : " + new java.util.Date(System.currentTimeMillis()));
			ResultSet rslt = stmt.executeQuery(strDetailSQL);
			System.out.println("Result Set Returned              : " + new java.util.Date(System.currentTimeMillis()));


			int iRowCount = 0;
			int iInsertCount = 0;
			int iSummaryCount = 0;
			
			String strHoldCompany = "";
			String strHoldProvider = "";
			String strHoldInvoice = "";
			String strHoldCancelInvoice = "";
			boolean boolSkipInvoice = false;
			
			Map hashProviderTotals = new HashMap();

			RowDIPInvoiceDetail objRow = new RowDIPInvoiceDetail();
						
			while (rslt.next()) {
				boolSkipInvoice = false;
				iRowCount++;
				objRow.flush();
//				if(iRowCount==1){
//					System.out.println("Selected First Row.");
//				}
				int i = 1;
				objRow.setCompanyCode(cleanse(rslt.getString(i++)));
				objRow.setProviderId(cleanse(rslt.getString(i++)));
				objRow.setInvoiceNum(cleanse(rslt.getString(i++)));
				objRow.setCancelInvoiceNum(cleanse(rslt.getString(i++)));
				objRow.setServDelivId(cleanse(rslt.getString(i++)));
				objRow.setCrossReferenceNum(cleanse(rslt.getString(i++)));
				objRow.setInvoiceAmount(rslt.getBigDecimal(i++));
				objRow.setAmountPaid(rslt.getBigDecimal(i++));
				objRow.setPaymentDueDate(rslt.getDate(i++));
				objRow.setItemId(cleanse(rslt.getString(i++)));
				objRow.setStatusCode(cleanse(rslt.getString(i++)));
				objRow.setDaysLate(rslt.getInt(i++));
				objRow.setLPCFlag(cleanse(rslt.getString(i++)));
				objRow.setLPCDate(rslt.getDate(i++));
				objRow.setRejectStatus(cleanse(rslt.getString(i++)));
				objRow.setRejectReasonCode(cleanse(rslt.getString(i++)));
				objRow.setRejectDate(rslt.getDate(i++));

//				if(iRowCount==1){System.out.println("Getting first TC Charges.");}

				// These are the nominal values, they will be adjusted later...
				String strDeposit = "";
				
				BigDecimal dInvoiceAmount = objRow.getInvoiceAmount();
				BigDecimal dAmountPaid = objRow.getAmountPaid();
				BigDecimal dAmountOwed = dInvoiceAmount.subtract(dAmountPaid);
				if ((dInvoiceAmount.compareTo(new BigDecimal(0.00)) < 0) && (dAmountPaid.compareTo(dInvoiceAmount) < 0)) {
					// Invoice is over-paid
					objRow.setAmountOwed(new BigDecimal(0.00));
					boolSkipInvoice = true;
				} else if ((dInvoiceAmount.compareTo(new BigDecimal(0.00)) > 0) && (dAmountPaid.compareTo(dInvoiceAmount)) > 0) {
					// Invoice is over-paid
					objRow.setAmountOwed(new BigDecimal(0.00));
					boolSkipInvoice = true;
				} else {
					objRow.setAmountOwed(dAmountOwed);
				}
//				if(iRowCount==1){System.out.println("First Amount Paid Calculated");}

				if ((objRow.getStatusCode().equalsIgnoreCase("CD"))
				 || (objRow.getStatusCode().equalsIgnoreCase("DI"))
				 || (objRow.getStatusCode().equalsIgnoreCase("D$"))
				 || (objRow.getStatusCode().equalsIgnoreCase("MG"))) {
				 	// Disputed invoices get counted at Amount Owed 
				 	// in case provider is disputing specific charges
					objRow.setDisputeFlag("Y"); 
					objRow.setDisputeAmount(objRow.getAmountOwed());	
				} else {
					objRow.setDisputeFlag(" ");
					objRow.setDisputeAmount(new BigDecimal(0.00));
				}
				
				if ((objRow.getDisputeFlag().equals("Y")) ||
					(objRow.getRejectStatus().equals("REP"))) {
					objRow.setTCAmount(new BigDecimal(0.00));
					objRow.setTCDiscountAmount(new BigDecimal(0.00));
					objRow.setTCNetAmount(new BigDecimal(0.00));
				} else {
//					if (objRow.getInvoiceAmount().compareTo(new BigDecimal(0.00)) > 0) {
//						pstmt.setString(1, objRow.getInvoiceNum());
//					} else {
//						pstmt.setString(1, objRow.getCancelInvoiceNum());
//					}
					
					if (objRow.getCancelInvoiceNum().equals("")) {
						pstmt.setString(1, objRow.getInvoiceNum());
					} else {
						pstmt.setString(1, objRow.getCancelInvoiceNum());
					}
					
					ResultSet prslt = pstmt.executeQuery();

					while (prslt.next()) {				
						java.math.BigDecimal tcAmt = prslt.getBigDecimal(1);
						tcAmt = tcAmt.setScale(2, BigDecimal.ROUND_DOWN);
						java.math.BigDecimal tcDiscAmt = prslt.getBigDecimal(2);
						tcDiscAmt = tcDiscAmt.setScale(2, BigDecimal.ROUND_DOWN);
						strDeposit = prslt.getString(3);

	//				if(iRowCount==1){System.out.println("First TC Charges received");}
						if (strDeposit.equals("Y")) {	
						//ignore this TC charge
						} else if (strDeposit.equals("P")) {
							objRow.setTCAmount(objRow.getTCAmount().add(tcAmt).subtract(objRow.getAmountPaid()));
							objRow.setTCNetAmount(objRow.getTCAmount());
						} else {
							BigDecimal dTCRatio = new BigDecimal(0.00);
							if (dInvoiceAmount.compareTo(new BigDecimal(0.00)) != 0) {
								dTCRatio = dAmountOwed.divide(dInvoiceAmount, BigDecimal.ROUND_DOWN);
								dTCRatio = dTCRatio.setScale(2, BigDecimal.ROUND_DOWN);
							}
							BigDecimal dTCAmount = tcAmt.multiply(dTCRatio);
							dTCAmount = dTCAmount.setScale(2, BigDecimal.ROUND_DOWN);
							BigDecimal dTCDiscAmount = tcDiscAmt.multiply(dTCRatio);
							dTCDiscAmount = dTCDiscAmount.setScale(2, BigDecimal.ROUND_DOWN);
							objRow.setTCAmount(objRow.getTCAmount().add(dTCAmount));
							objRow.setTCDiscountAmount(objRow.getTCDiscountAmount().add(dTCDiscAmount));
							objRow.setTCNetAmount(objRow.getTCAmount().add(objRow.getTCDiscountAmount()));
					    }
					}
				}
//				if(iRowCount==1){System.out.println("First TC Charges calculated");}

				
				if (objRow.getRejectStatus().equalsIgnoreCase("REP")) {
					objRow.setRejectAmount(objRow.getInvoiceAmount());
				} else {
					objRow.setRejectAmount(new BigDecimal(0.00));
				}
				
				strHoldCompany = objRow.getCompanyCode();
				strHoldProvider = objRow.getProviderId();
				strHoldInvoice = objRow.getInvoiceNum();
				strHoldCancelInvoice = objRow.getCancelInvoiceNum();
					
				if (!boolSkipInvoice) {
//					if (iRowCount==1){System.out.println("Processing First Row");}
					if (objRow.getDaysLate() >= 5) {
						if (!boolSkipInvoice) {
							iInsertCount += insert(objRow, istmt);
//							if(iInsertCount==1){System.out.println("First row inserted");}
						}
					} else {
						iSummaryCount++;
						addCurrentInvoice(hashProviderTotals, objRow);
					}
				}

//				if(iRowCount==1){System.out.println("First Row Complete");}
				displayStatus(iRowCount, iInsertCount, iSummaryCount);
			}
			System.out.println("Query Execution Complete         : " + new java.util.Date(System.currentTimeMillis()));
			
			System.out.println("Inserting Summary Rows           : " + new java.util.Date(System.currentTimeMillis()));
			Collection collProviderTotals = hashProviderTotals.values();
			Iterator iter = collProviderTotals.iterator();
			PreparedStatement stmtSummary = connOracle.prepareStatement(strSummarySQL);
			while (iter.hasNext()) {
				RowDIPCurrentInvoiceSummary rowSummary = (RowDIPCurrentInvoiceSummary) iter.next();
				stmtSummary.setInt(1, iRunKey);
				stmtSummary.setString(2, cleanse(rowSummary.getCompanyCd()));
				stmtSummary.setString(3, cleanse(rowSummary.getProviderID()));
				stmtSummary.setDouble(4, rowSummary.getCreditInvAmt());				
				stmtSummary.setDouble(5, rowSummary.getDebitInvAmt());				
				stmtSummary.setDouble(6, rowSummary.getNetInvAmt());				
				stmtSummary.setDouble(7, rowSummary.getDispAmt());				
				stmtSummary.setDouble(8, rowSummary.getRejectAmt());				
				stmtSummary.setDouble(9, rowSummary.getTCNetAmt());				
				stmtSummary.setDouble(10, rowSummary.getTCCreditNetAmt());				
				stmtSummary.setDouble(11, rowSummary.getTCCreditDiscAmt());				
				stmtSummary.setDouble(12, rowSummary.getTCCreditAmt());				
				stmtSummary.setDouble(13, rowSummary.getTCDebitNetAmt());				
				stmtSummary.setDouble(14, rowSummary.getTCDebitDiscAmt());		
				stmtSummary.setDouble(15, rowSummary.getTCDebitAmt());		
				//System.out.println("Company Code : " + rowSummary.getCompanyCd());
				//System.out.println("Provider ID  : " + rowSummary.getProviderID());		
				stmtSummary.executeUpdate();		
			}
			stmtSummary.close();

		} catch (SQLException sqle) {
			sqle.printStackTrace();
			boolReturn = false;
		} finally {
			try {
				istmt.close();
				pstmt.close();
				stmt.close();
			} catch (SQLException sqle2) {}
			return boolReturn;
		}
	}

	private boolean addCurrentInvoice(Map hashTotals, RowDIPInvoiceDetail rowDetail) {
		RowDIPCurrentInvoiceSummary rowSummary = null;
		if (!hashTotals.containsKey(rowDetail.getProviderId() + "#" + rowDetail.getCompanyCode())) {
			// Initialize Provider Row
			rowSummary = new RowDIPCurrentInvoiceSummary();
			rowSummary.setProviderID(rowDetail.getProviderId().trim());
			rowSummary.setCompanyCd(rowDetail.getCompanyCode().trim());
		} else {
			rowSummary = (RowDIPCurrentInvoiceSummary) hashTotals.get(rowDetail.getProviderId() + "#" + rowDetail.getCompanyCode());
		}
		
		rowSummary.setNetInvAmt(rowSummary.getNetInvAmt() + rowDetail.getAmountOwed().doubleValue());
		
		if ((rowDetail.getDisputeFlag().equals("Y")) ||
		    (rowDetail.getRejectStatus().equals("REP"))) {
				if (rowDetail.getDisputeFlag().equals("Y")) {
					rowSummary.setDispAmt(rowSummary.getDispAmt() + rowDetail.getDisputeAmount().doubleValue());
				}

				if (rowDetail.getRejectStatus().equals("REP")) {
					rowSummary.setRejectAmt(rowSummary.getRejectAmt() + rowDetail.getRejectAmount().doubleValue());				
				}
		} else {
			if (rowDetail.getAmountOwed().compareTo(new BigDecimal(0.00)) < 0) {
				rowSummary.setCreditInvAmt(rowSummary.getCreditInvAmt() + rowDetail.getAmountOwed().doubleValue());
			} else {
				rowSummary.setDebitInvAmt(rowSummary.getDebitInvAmt() + rowDetail.getAmountOwed().doubleValue());
			}

			if (rowDetail.getTCAmount().compareTo(new BigDecimal(0.00)) < 0) {
				rowSummary.setTCCreditAmt(rowSummary.getTCCreditAmt() + rowDetail.getTCAmount().doubleValue());
				rowSummary.setTCCreditDiscAmt(rowSummary.getTCCreditDiscAmt() + rowDetail.getTCDiscountAmount().doubleValue());
				rowSummary.setTCCreditNetAmt(rowSummary.getTCCreditNetAmt() + rowDetail.getTCNetAmount().doubleValue());
			} else {
				rowSummary.setTCDebitAmt(rowSummary.getTCDebitAmt() + rowDetail.getTCAmount().doubleValue());
				rowSummary.setTCDebitDiscAmt(rowSummary.getTCDebitDiscAmt() + rowDetail.getTCDiscountAmount().doubleValue());
				rowSummary.setTCDebitNetAmt(rowSummary.getTCDebitNetAmt() + rowDetail.getTCNetAmount().doubleValue());
			}

			rowSummary.setTCNetAmt(rowSummary.getTCNetAmt() + rowDetail.getTCNetAmount().doubleValue());
		}
		hashTotals.put(rowDetail.getProviderId() + "#" + rowDetail.getCompanyCode(), rowSummary);
		
		return true;
	}

	private int insert(RowDIPInvoiceDetail objRow, PreparedStatement istmt) throws SQLException {
		int j = 1;
		istmt.setInt(j++,iRunKey);
		istmt.setString(j++, objRow.getCompanyCode());
		istmt.setString(j++, objRow.getProviderId());
		istmt.setString(j++, objRow.getInvoiceNum());
		istmt.setString(j++, objRow.getCancelInvoiceNum());
		istmt.setString(j++, objRow.getServDelivId());
		istmt.setString(j++, objRow.getCrossReferenceNum());
		istmt.setBigDecimal(j++, objRow.getInvoiceAmount());
		istmt.setBigDecimal(j++, objRow.getAmountPaid());
		istmt.setBigDecimal(j++, objRow.getAmountOwed());
		istmt.setDate(j++, objRow.getPaymentDueDate());
		istmt.setString(j++, objRow.getItemId());
		istmt.setString(j++, objRow.getStatusCode());
		istmt.setInt(j++, objRow.getDaysLate());
		istmt.setString(j++, objRow.getLPCFlag());
		istmt.setDate(j++, objRow.getLPCDate());
		istmt.setString(j++, objRow.getRejectStatus());
		istmt.setString(j++, objRow.getRejectReasonCode());
		istmt.setDate(j++, objRow.getRejectDate());
		istmt.setString(j++, objRow.getDisputeFlag());
		istmt.setBigDecimal(j++, objRow.getDisputeAmount());
		istmt.setBigDecimal(j++, objRow.getTCAmount());
		istmt.setBigDecimal(j++, objRow.getTCDiscountAmount());
		istmt.setBigDecimal(j++, objRow.getTCNetAmount());
		istmt.setBigDecimal(j++, objRow.getRejectAmount());
		return istmt.executeUpdate();
	}

	private int insert(RowDIPWeeklySummary objRow, PreparedStatement istmt) throws SQLException {
		int j = 1;
		istmt.setInt(j++,iRunKey);
		istmt.setString(j++, objRow.getCompanyCd());
		istmt.setString(j++, objRow.getProviderID());
		istmt.setDate(j++, objRow.getLastPayDate()); 
		istmt.setDouble(j++, objRow.getTotalBillAmt());
		return istmt.executeUpdate();
	}


	public static void main(String[] args) {
		DIPDataExtraction dip = new DIPDataExtraction();
		
		if (args.length == 3) {
			dip.executeReport(args[0], args[1], args[2]);
		} else if (args.length == 2) {
			dip.executeReport(args[0], args[1], "");
		} else {
			System.out.println("Invalid number of arguments");
		}
	}

	private int getEnvOracle() {
		try {
			String LPAR = InetAddress.getLocalHost().getHostName().trim().toUpperCase();	
			if (LPAR.startsWith("COCJ")) {
				return DBConnectionManager.ENV_PROD;	
			} else if (LPAR.startsWith("COCM")) {
				return DBConnectionManager.ENV_DEV;	
			} else {
				return DBConnectionManager.ENV_DEV;	
			}	
		} catch (UnknownHostException uhe) {
			return -1;
		}
	}

	private int getEnvDB2() {
		try {
			String LPAR = InetAddress.getLocalHost().getHostName().trim().toUpperCase();	
			if (LPAR.startsWith("COCJ")) {
				return DBConnectionManager.ENV_PROD;	
			} else if (LPAR.startsWith("COCM")) {
				return DBConnectionManager.ENV_DEV;	
			} else {
				return DBConnectionManager.ENV_DEV;	
			}	
		} catch (UnknownHostException uhe) {
			return -1;
		}
	}
	
	private int getPlatform() {
		try {
			String LPAR = InetAddress.getLocalHost().getHostName().trim().toUpperCase();	
			if (LPAR.startsWith("COCJ")) {
				return DBConnectionManagerBatch.PLATFORM_MAINFRAME;	
			} else if (LPAR.startsWith("COCM")) {
				return DBConnectionManagerBatch.PLATFORM_MAINFRAME;	
			} else {
				return DBConnectionManagerBatch.PLATFORM_OPEN;	
			}	
		} catch (UnknownHostException uhe) {
			return -1;
		}
	}

	/**
	 * Method cleanse.
	 * @param argstrValue
	 * @return String
	 */
	protected String cleanse(String argstrValue) {
		try {
			if(argstrValue.equals(null)) {
				return "";
			} else {
				return argstrValue.trim();
			}	
		} catch (NullPointerException npe) {
			return "";
		}	
	}

	private java.sql.Date getLastBusinessDay() {
		Calendar cal = new GregorianCalendar();
		if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
			cal.add(Calendar.DAY_OF_MONTH, -2);
		} else if (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
			cal.add(Calendar.DAY_OF_MONTH, -1);
		}
		java.sql.Date sqlDate = new java.sql.Date(cal.getTime().getTime());
		return sqlDate;
	}

}
