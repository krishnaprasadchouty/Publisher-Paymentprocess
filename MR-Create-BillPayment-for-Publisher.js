/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/record', "N/search",'N/file','N/format','N/runtime','N/email'],

		function(record, search, file, format, Papa, file, runtime, email) {


	/**
	 * Marks the beginning of the Map/Reduce process and generates input data.
	 *
	 * @typedef {Object} ObjectRef
	 * @property {number} id - Internal ID of the record instance
	 * @property {string} type - Record type id
	 *
	 * @return {Array|Object|Search|RecordRef} inputSummary
	 * @since 2015.1
	 */
	//START: Creating a Search to get the list of Publisher Bills that needs to be Processed on 15 of every month.
	//Based on the Due Date of the Bill the Below search will get the results and process the Bill Payments
	function getInputData() {
		try{
			var vbBill;
			var publisherName;
			var vbDetails = [];
			var vendorbillSearchObj = search.create({
				type: "vendorbill",
				filters:
					[
					 ["type","anyof","VendBill"],// Type is Vendor Bill
					 "AND", 
					 ["mainline","is","T"], 
					 "AND", 
					 ["custbody_publisher_vendor_bill","is","T"], //Only Publisher Bill
					 "AND", 
					 ["approvalstatus","anyof","2"],  // Bill should be Approved
					 "AND", 
					 ["status","anyof","VendBill:A"], //Status should be Pending Bill/Open
					 "AND", 
					 ["duedate","onorbefore","today"], // Due Date on or before todays date
//					 "AND",["internalid","anyof","94043","94045","94044"]
					 ],
					 columns:
						 [
						  search.createColumn({name: "internalid", label: "Internal ID"}),
						  search.createColumn({
							  name: "entity",
							  sort: search.Sort.ASC,
							  label: "Name"
						  }),
						  search.createColumn({
							  name: "transactionnumber",
							  sort: search.Sort.ASC,
							  label: "Transaction Number"
						  }),
						  search.createColumn({name: "approvalstatus", label: "Approval Status"}),
						  search.createColumn({name: "statusref", label: "Status"}),
						  search.createColumn({name: "trandate", label: "Date"}),
						  search.createColumn({name: "duedate", label: "Due Date/Receive By"}),
						  search.createColumn({name: "fxamount", label: "Amount (Foreign Currency)"})
						  ]
			});
			var searchResultCount = vendorbillSearchObj.runPaged().count;
			log.debug("vendorbillSearchObj result count",searchResultCount);
			vendorbillSearchObj.run().each(function(result){
				vbBill = result.getValue({name: "internalid"});
				publisherName = result.getValue({name: "entity"});
				if(vbBill && publisherName){
					vbDetails.push({
						'vbBill':vbBill,
						'publisherName':publisherName
					});
				}
				return true;
			});
			log.debug('vbDetails:',vbDetails);
			return vbDetails;
		}catch(e){
			log.error('Error in getInputData',e);
		}
	}//END: Creating a Search to get the list of Publisher Bills that needs to be Processed on 15 of every month.

	/**
	 * Executes when the map entry point is triggered and applies to each key/value pair.
	 *
	 * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
	 * @since 2015.1
	 */

	//START: Passing the Publisher Bill ID's to the Next stage(Reduce) to create a Bill Payment
	function map(context) {
		try{
			var vbData = JSON.parse(context.value);
			var vbId = vbData.vbBill;
			log.debug('vbId:',vbId);
			var vendName = vbData.publisherName;
			log.debug('vendName:',vendName);
			if(vbId && vendName){
				context.write({
					key: vendName,
					value : {
						'vbId':vbId
					}
				});	
			}
		}catch(e){
			log.error('Error in map',e);
		}
	}//END: Passing the Publisher Bill ID's to the Next stage(Reduce) to create a Bill Payment

	/**
	 * Executes when the reduce entry point is triggered and applies to each group.
	 *
	 * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
	 * @since 2015.1
	 */

	//START: Create a Bill Payment for the Publisher based on the Due Date.
	function reduce(context) {
		try{
			var formId;
			var myEnvType = JSON.stringify(runtime.envType);
			log.debug('myEnvType:',myEnvType);
			if(myEnvType === runtime.EnvType.PRODUCTION){// 150 = SNII Publisher Payment Form //151 for SB1
				formId = 150;
			}else{
				formId = 151;
			}
			var billId;
			var vendorName = context.key;
			log.debug('Reduce-vendorName:',vendorName);

			var contentArr = context.values;
			log.debug('contentArr 1:',contentArr);
			log.debug('contentArr 1 Length :',contentArr.length);

			var vpRec = record.create({
				type: record.Type.VENDOR_PAYMENT,
				isDynamic: true
			});
			log.debug('myEnvType:',myEnvType+' and Form ID is: '+formId);
			vpRec.setValue({
				fieldId: 'customform',
				value: formId 
			});
			vpRec.setValue({
				fieldId: 'entity',
				value: vendorName
			});

			vpRec.setValue({
				fieldId: 'subsidiary',
				value: 1 // 1 = SmartNews Co., (Parent Company)
			});
			vpRec.setValue({
				fieldId: 'currency',
				value: 2 //2 = USD
			});
			vpRec.setValue({
				fieldId: 'exchangerate',
				value: 1.00
			});

			vpRec.setValue({
				fieldId: 'account',
				value: 945 // 945 = Bank Account Number: 101343 (101300 Bank : Cash and deposits : Savings accounts
			});
			vpRec.setValue({
				fieldId: 'department',
				value: 81 // 81 = A00057 Business Operations 1 : A00063 US Media Business Develo: A00065 US Partner Relations
			});

			var scriptExecDate = new Date();
			var currDate = format.parse({value: scriptExecDate, type: format.Type.DATE});
			log.audit('Final (Bill Payment) Date:',currDate);
			vpRec.setValue({
				fieldId: 'trandate',
				value: currDate
			});

			//Select the Publisher bills only if the Due Date is 15th of current month
			for(var cA = 0; cA < contentArr.length; cA++){
				var rContentArr = JSON.parse(contentArr[cA]);
				log.debug('rContentArr cA:',rContentArr);
				billId = rContentArr.vbId;
				log.audit('billId:',billId);

				var lineCount = vpRec.getLineCount({sublistId: 'apply'});
				log.audit('1-lineCount',lineCount);

				var billLineNum = vpRec.findSublistLineWithValue({sublistId: 'apply',fieldId: 'internalid',value: billId}); // Internal ID of Bill
				log.audit('billLineNum',billLineNum);

				if(billLineNum != -1){
					var lineNum = vpRec.selectLine({
						sublistId: 'apply',
						line: billLineNum
					});
					vpRec.setCurrentSublistValue({
						sublistId: 'apply',
						fieldId: 'apply',
						value: true
					});
					log.debug('***Bill Applied***');
				}
			}
			var vpRecSave = vpRec.save(true);
			log.audit('**** Vend Pymt Rec Saved # :'+vpRecSave);
		}catch(e){
			log.error('Error in reduce',e);
		}
	}//END: Create a Bill Payment for the Publisher based on the Due Date.


	/**
	 * Executes when the summarize entry point is triggered and applies to the result set.
	 *
	 * @param {Summary} summary - Holds statistics regarding the execution of a map/reduce script
	 * @since 2015.1
	 */
	function summarize(summary) {
		try{

		}catch(e){
			log.error('Error in summarize',e);
		}
	}

	return {
		getInputData: getInputData,
		map: map,
		reduce: reduce,
		summarize: summarize
	};

});
