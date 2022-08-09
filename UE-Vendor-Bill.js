/**
 * @NApiVersion 2.x
 * @NScriptType UserEventScript
 * @NModuleScope SameAccount
 */
define(['N/format', 'N/record', 'N/search'],

		function(format, record, search) {

	function beforeSubmit(scriptContext) {
		try{
			var recType = scriptContext.newRecord.type;
			var priceTierRec = scriptContext.newRecord
			if(recType == 'customrecord_sn_pricing_tier_list'){
				var pubName = priceTierRec.getValue({fieldId: "custrecord_pubr_item_name_num"});
				log.debug('pubName:',pubName);
				if(pubName){
					var vendorId;
					var vendorSearchObj = search.create({
						type: "vendor",
						filters:
							[
							 ["custentity_dps_snii_publisher_item_name","is",pubName], 
							 "AND", 
							 ["isinactive","is","F"]
							 ],
							 columns:
								 [
								  search.createColumn({name: "internalid", label: "Internal ID"})
								  ]
					});
					var searchResultCount = vendorSearchObj.runPaged().count;
					log.debug("vendorSearchObj result count",searchResultCount);

					vendorSearchObj.run().each(function(result){
						vendorId = result.getValue('internalid');
						priceTierRec.setValue({
							fieldId: "custrecord_pricing_tier_publisher",
							value: vendorId
						});
						return false;
					});
				}
			}

		}catch(e){
			log.error('Error in BeforeSubmit:',e);
		}
	}


	function afterSubmit(scriptContext) {
		try{
			var recType = scriptContext.newRecord.type;
			var recId = scriptContext.newRecord.id;

			if(scriptContext.type == 'create'){ // ****remove the Edit Context in PROD // || scriptContext.type == 'edit'

				if(recId && recType == 'vendorbill'){ //Validating the Record Type is Bill
					//This function will set the "Will Be Paid By" date only for Publisher Bills and to update the Bank Account Details
					updatePublisherBill(recId,record,format,search);

				}else if(recId && recType == 'vendorpayment'){
					addPublisherBillDetails(recId,record,format,search);
				}

			}
		}catch(e){
			log.error('Error in afterSubmit function: ',e);
		}


		//START: This function will set the "Will Be Paid By" date only for Publisher Bills and to update the Bank Account Details
		function updatePublisherBill(recId,record,format,search){
			try{
				var publisherBill = false;
				var recObj = record.load({type: 'vendorbill', id: recId}); //Load Bill
				var vendId = recObj.getValue({fieldId: "entity"});
				log.debug('vendId:',vendId);

				publisherBill = recObj.getValue({fieldId: "custbody_publisher_vendor_bill"});
				log.debug('publisherBill:',publisherBill);

				if(publisherBill){ //Validate the Bill is Publisher Bill? or Not?
					var dueDate = recObj.getValue({fieldId: "duedate"});
					log.debug('dueDate:',dueDate);

					if(dueDate){ //Based on the Due Date value, getting the Month End Date and this will be used in Advanced PDF.
						var t = new Date(dueDate);
						var updatedDate = new Date(t.getFullYear(), t.getMonth() + 1, 0, 00, 00, 00);
						updatedDate = format.parse({value: updatedDate, type: format.Type.DATE});
						log.debug('updatedDate:',updatedDate);

						recObj.setValue({ //Updating the Will be Paid by date value in Bills 
							fieldId: "custbody_will_be_paid_by",
							value: updatedDate
						});

						//Calling function to set the Bank Account Details in Publisher Bills.
						setBankAccountDetails(vendId,search,recObj);
						log.debug('Bank Details Updated in Vendor Bill');


						var vbSave = recObj.save(true);
						log.debug('** Vendor Bill Saved # '+vbSave+ ' **');
					}
				}else{
					log.audit('Bill is not for Publisher');
				}

			}catch(e){
				log.error('Error in updatePublisherBill Function:',e);
			}
		}//END: This function will set the "Will Be Paid By" date only for Publisher Bills and to update the Bank Account Details


		//START: Update Bank Account Details
		function setBankAccountDetails(vendId,search,recObj){
			try{
				//New Logic to set the Bank Account Details:
				var vendRec = search.lookupFields({
					type: 'vendor',
					id: vendId,
					columns: ['companyname']
				})
				var vendCompanyName = vendRec.companyname;
				log.debug('vendCompanyName',vendCompanyName);

				if(vendCompanyName){
					var bankDetailAcctSearch = search.create({
						type: "customrecord_2663_entity_bank_details",
						filters:
							[["name","is",vendCompanyName]],
							columns:
								[
								 search.createColumn({name: "name", label: "Beneficiary Name"}),
								 search.createColumn({name: "custrecord_2663_entity_acct_name", label: "Bank Name"}),
								 search.createColumn({name: "custrecord_2663_entity_address1", label: "Address1"}),
								 search.createColumn({name: "custrecord_2663_entity_address2", label: "Address2"}),
								 search.createColumn({name: "custrecord_2663_entity_address3", label: "Address3"}),
								 search.createColumn({name: "custrecord_2663_entity_city", label: "City"}),
								 search.createColumn({name: "custrecord_2663_entity_state", label: "State/Province"}),
								 search.createColumn({name: "custrecord_2663_entity_zip", label: "Zip"}),
								 search.createColumn({name: "custrecord_2663_entity_country", label: "Country"}),
								 search.createColumn({name: "custrecord_2663_entity_acct_no", label: "ACCOUNT NUMBER"}),
								 search.createColumn({name: "custrecord_dps_ach_number", label: "ACH"}),
								 search.createColumn({name: "custrecord_dps_bank_routing_number_aba", label: "Bank Routing Number (ABA)"}),
								 search.createColumn({name: "custrecord_2663_entity_swift", label: "Swift Code"}),
								 search.createColumn({name: "custrecord_2663_entity_iban", label: "IBAN"}),
								 search.createColumn({name: "custrecord_dps_bank_details_sort_code", label: "Sort Code"}),
								 search.createColumn({name: "custrecord_2663_entity_bic", label: "BIC"})

								 ]
					});
					var searchResultCount = bankDetailAcctSearch.runPaged().count;
					log.debug("bankDetailAcctSearch result count",searchResultCount);
					var fAddress = '';
					bankDetailAcctSearch.run().each(function(result){

						var beneficiaryName = result.getValue({name: "name"});
						var bankName = result.getValue({name: "custrecord_2663_entity_acct_name"});
						//Bank Address
						var a1 = result.getValue({name: "custrecord_2663_entity_address1"});
						var a2 = result.getValue({name: "custrecord_2663_entity_address2"});
						var a3 = result.getValue({name: "custrecord_2663_entity_address3"});
						var aCity = result.getValue({name: "custrecord_2663_entity_city"});
						var aState = result.getValue({name: "custrecord_2663_entity_state"});
						var aZip = result.getValue({name: "custrecord_2663_entity_zip"});
						var aCountry = result.getText({name: "custrecord_2663_entity_country"});
						if(a1){
							fAddress = a1;
						}
						if(a2){
							fAddress = fAddress+'|'+a2;
						}
						if(a3){
							fAddress = fAddress+'|'+a3;
						}
						if(aCity){
							fAddress = fAddress+'|'+aCity;
						}
						if(aState){
							fAddress = fAddress+'|'+aState;
						}
						if(aZip){
							fAddress = fAddress+'|'+aZip;
						}
						if(aCountry){
							fAddress = fAddress+'|'+aCountry;
						}
						if(fAddress){
							fAddress = fAddress+'{#}'
						}
						log.debug('fAddress:',fAddress);
						
						var acctNumber = result.getValue({name: "custrecord_2663_entity_acct_no"});
						var acH = result.getValue({name: "custrecord_dps_ach_number"});
						var bankRoutingNumber = result.getValue({name: "custrecord_dps_bank_routing_number_aba"});//Bank Routing Number (ABA)
						var swiftCode = result.getValue({name: "custrecord_2663_entity_swift"});
						var iBan = result.getValue({name: "custrecord_2663_entity_iban"});
						var sortCode = result.getValue({name: "custrecord_dps_bank_details_sort_code"});
						var bicNum = result.getValue({name: "custrecord_2663_entity_bic"});
							
						

						
						log.debug("Bank Account Details:",beneficiaryName+' , '+bankName+' , '+acctNumber+' , '+bankRoutingNumber+' , '+swiftCode);

						if(beneficiaryName){
							recObj.setValue({fieldId: "custbody_sni_beneficiary_name",value: beneficiaryName});
						}
						if(bankName){
							recObj.setValue({fieldId: "custbody_sni_bank_name",value: bankName});
						}
						if(fAddress){
							recObj.setValue({fieldId: "custbody_sni_bank_address",value: fAddress});
						}
						if(acctNumber){
							recObj.setValue({fieldId: "custbody_sni_account_number",value: acctNumber});
						}
						if(acH){
							recObj.setValue({fieldId: "custbody_sni_ach_number",value: acH});
						}
						if(bankRoutingNumber){
							recObj.setValue({fieldId: "custbody_sni_aba_routing_number",value: bankRoutingNumber});
						}
						if(swiftCode){
							recObj.setValue({fieldId: "custbody_sni_swift_code",value: swiftCode});
						}
						if(iBan){
							recObj.setValue({fieldId: "custbody_sni_iban",value: iBan});
						}
						if(sortCode){
							recObj.setValue({fieldId: "custbody_sni_sort_code",value: sortCode});
						}
						if(bicNum){
							recObj.setValue({fieldId: "custbody_sni_bic",value: bicNum});
						}


						return false;
					});

				}
			}catch(e){
				log.error('Error in',e);
			}
		}

		//END: Update Bank Account Details

		//START: To set the Publisher Bill Details in the Bill Payment, for Advanced PDF
		function addPublisherBillDetails(recId,record,format,search){
			try{
				var month = ["January","February","March","April","May","June","July","August","September","October","November","December"];
				var pubBillDetails = '';
				var billAmtDetails = '';

				var itemName, websiteName, websiteURL, pubBillDate = '';
				var pagesView, billAmt = 0;

				var recObj = record.load({type: 'vendorpayment', id: recId}); //Load Vendor Payment
				var vendId = recObj.getValue({fieldId: "entity"});
				log.debug('vendId:',vendId);

				setBankAccountDetails(vendId,search,recObj);
				log.debug('Bank Details Updated in Bill Payment');

				var vpRec = search.lookupFields({
					type: 'vendor',
					id: vendId,
					columns: ['category']
				})
				var vendCategory = vpRec.category;
				log.debug('vendCategory',vendCategory);
				vendCategory = vendCategory[0].value;
				log.debug('vendCategory',vendCategory);
				if(vendCategory == 5 || vendCategory == 6){
					var lineCount = recObj.getLineCount({sublistId: 'apply'});
					log.debug('lineCount',lineCount);
					var billArr = [];
					for(var pL=0; pL<lineCount; pL++){
						var billApplied = recObj.getSublistValue({
							sublistId: 'apply',
							fieldId: 'apply',
							line: pL
						});
						log.audit('billApplied',billApplied);
						if(billApplied == true){
							var billID = recObj.getSublistValue({
								sublistId: 'apply',
								fieldId: 'internalid',
								line: pL
							});
							billArr.push(billID);
						}

					}
					log.debug('billArr',billArr);
					if(billArr.length>0){
						var publBillSearch = search.create({
							type: "vendorbill",
							filters:
								[
								 ["type","anyof","VendBill"], 
								 "AND", 
								 ["mainline","is","F"], 
								 "AND", 
								 ["internalid","anyof",billArr], 
								 "AND", 
								 ["taxline","is","F"], 
								 "AND", 
								 ["cogs","is","F"]
								 ],
								 columns:
									 [
									  search.createColumn({name: "trandate",sort: search.Sort.ASC,label: "Date"}),

									  search.createColumn({name: "item", label: "Item"}),
									  search.createColumn({name: "custcol_website_name", label: "Website Name"}),
									  search.createColumn({name: "custcol_dp_pubwebsiteurl", label: "Website URL"}),
									  search.createColumn({name: "custcol_pages_views", label: "Pages Views"}),
									  search.createColumn({name: "fxamount", label: "Amount (Foreign Currency)"})
									  ]
						});
						var publBillSearchCount = publBillSearch.runPaged().count;
						log.debug("publBillSearchCount",publBillSearchCount);
						publBillSearch.run().each(function(result){
							// .run().each has a limit of 4,000 results
							pubBillDate = result.getValue({name: "trandate"}) || '';
							pubBillDate = new Date(pubBillDate);

							var yearYYYY = pubBillDate.getFullYear();
							pubBillDate = month[pubBillDate.getMonth()]+' '+yearYYYY;

							itemName = result.getText({name: "item"}) || '';

							websiteName = result.getValue({name: "custcol_website_name"}) || '';

							websiteURL = result.getValue({name: "custcol_dp_pubwebsiteurl"}) || '';

							pagesView = result.getValue({name: "custcol_pages_views"}) || '';
							log.debug("pagesView 1, "+'for Bill # date and website name :'+pubBillDate+'; URL: '+websiteURL+'; '+pagesView);
							pagesView = format.format({value: pagesView, type: format.Type.CURRENCY});
							log.debug("pagesView 2",pagesView);
							pagesView = pagesView.split('.');
							log.debug("pagesView 3",pagesView);
							pagesView = pagesView[0];

							billAmt = result.getValue({name: "fxamount"}) || '';
							billAmt = format.format({value: billAmt, type: format.Type.CURRENCY});
							log.debug('Search Results:','pubBillDate: '+pubBillDate+' ;itemName: '+itemName+' ;websiteName: '+websiteName+' ;websiteURL: '+websiteURL+' ;pagesView: '+pagesView+' ;billAmt: '+billAmt)
							//to set the Lines in the PDF
							pubBillDetails = pubBillDetails+pubBillDate+'|'+itemName+'|'+websiteName+'|'+websiteURL+'|'+pagesView+'{#}'
							//to set the Description in the PDF
							if(billAmt != '0.00'){
								billAmtDetails = billAmtDetails+pubBillDate+'|'+billAmt+'{#}'
							}
							return true;
						});
						log.debug("Final Data-pubBillDetails",pubBillDetails);

						log.debug("Final Data-billAmtDetails",billAmtDetails);

						if(pubBillDetails){
							recObj.setValue({fieldId: "custbody_publisher_url_hidden",value: pubBillDetails});

							recObj.setValue({fieldId: "custbody_publisher_amt_hidden",value: billAmtDetails});
						}

						var billIDNum = billArr[0];
						var billRec = record.load({type: 'vendorbill', id: billIDNum});
						var willBePaid = billRec.getValue({fieldId: "custbody_will_be_paid_by"});						
						willBePaid = format.parse({value: willBePaid, type: format.Type.DATE});
						log.debug('willBePaid:',willBePaid);

						if(willBePaid){
							recObj.setValue({ //Updating the Will be Paid by date value in Bills 
								fieldId: "custbody_will_be_paid_by",
								value: willBePaid
							});
						}

						recObj.save(true);
					}
				}
			}catch(e){
				log.error('Error in addPublisherBillDetails function:',e);
			}
		}//END: To set the Publisher Bill Details in the Bill Payment, for Advanced PDF
	}

	return {
		beforeSubmit: beforeSubmit,
		afterSubmit: afterSubmit
	};

});
