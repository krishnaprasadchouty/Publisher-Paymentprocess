/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
var dd;
define(['N/record', "N/search",'N/file','N/format','./papaparse.min.js','N/runtime','N/email'],

		function(record, search, file, format, Papa, runtime, email) {

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

	//START: Search to get the File from the NetSuite File Cabinet to process the Publisher Bills
	function getInputData() {
		try{
			var pendingFileFolder;
			var myEnvType = JSON.stringify(runtime.envType);
			log.debug('myEnvType:',myEnvType);
			if(myEnvType === runtime.EnvType.PRODUCTION){ // 723 - for SB2 and 733 for PROD
				pendingFileFolder = 733;
			}else{
				pendingFileFolder = 723;
			}
			log.debug('myEnvType:',myEnvType+' and Folder ID is: '+pendingFileFolder);
			var vbFileId;
			var vbFileArr = [];
			var vbProcessingFolder = search.create({
				type: "folder",
				filters:
					[
					 ["internalid","anyof",pendingFileFolder]
					 ],
					 columns:
						 [
						  search.createColumn({name: "numfiles", label: "# of Files"}),
						  search.createColumn({
							  name: "internalid",
							  join: "file",
							  sort: search.Sort.ASC,
							  label: "Internal ID"
						  })
						  ]
			});
			var vbProcessingFolderFileCount = vbProcessingFolder.runPaged().count;
			log.debug("vbProcessingFolderFileCount",vbProcessingFolderFileCount);
			vbProcessingFolder.run().each(function(result){
				vbFileId = result.getValue({name: "internalid",join: "file"});
				if(vbFileId){
					vbFileArr.push({
						'vbFileId':vbFileId
					});

				}
				return false;
			});
			log.debug('vbFileArr:',vbFileArr);
			return vbFileArr;
		}catch(e){
			log.error('Error in getInputData',e);
		}
	}//END: Search to get the File from the NetSuite File Cabinet to process the Publisher Bills

	/**
	 * Executes when the map entry point is triggered and applies to each key/value pair.
	 *
	 * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
	 * @since 2015.1
	 */

	//START: Sending the Data to next stage(Reduce) by creating a Key Value pair based on the Publisher Name and File ID
	function map(context) {
		try{
			var fileObj = JSON.parse(context.value);
			var fileId = fileObj.vbFileId;
//			log.debug('fileId:',fileId);

			if(fileId){
				var strContent = file.load({id: fileId}).getContents();
				var data = Papa.parse(strContent);
//				log.debug('data 1: ',data);
				var dataContent = data.data;
//				log.debug('dataContent Length: ',dataContent.length);
//				log.debug('dataContent: ',dataContent);
				var vbDate, publisherName, siteName, siteURL, smartView = '';
				for(var dC = 1; dC < dataContent.length; dC++){
					vbDate = dataContent[dC][0];
					publisherName = dataContent[dC][1];
					siteName = dataContent[dC][2];
					siteURL = dataContent[dC][3];
					smartView = dataContent[dC][4];
					log.debug('FileContent:','fileId: '+fileId+' = vbDate: '+vbDate+'; publisherName: '+publisherName+'; siteName: '+siteName+'; siteURL: '+siteURL+'; smartView: '+smartView)
					if(publisherName && siteURL && smartView && vbDate){
						context.write({
							key: publisherName+'_'+fileId,
							value : {
								'vbDate':vbDate,
								'publisherName':publisherName,
								'siteName':siteName,
								'siteURL':siteURL,
								'smartView':smartView
							}
						});	
					}
				}
			}
		}catch(e){
			log.error('Error in map',e);
		}
	}//END: Sending the Data to next stage(Reduce) by creating a Key Value pair based on the Publisher Name and File ID

	/**
	 * Executes when the reduce entry point is triggered and applies to each group.
	 *
	 * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
	 * @since 2015.1
	 */
	function reduce(context) {
		try{
			var pubBillID;
			var myEnvType = JSON.stringify(runtime.envType);
			log.debug('myEnvType:',myEnvType);
			if(myEnvType === runtime.EnvType.PRODUCTION){//148 = SNII Publisher Vendor Bill form for SB2 and *** 149 for PROD ***
				pubBillID = 149;
			}else{
				pubBillID = 148;
			}
			log.debug('myEnvType:',myEnvType+' and Form ID is: '+pubBillID);
			var errorFound = 0; //Flag to create a Error File
			var errorMessage = '';
			var errorData = '';
			var reduceKey = context.key;
//			log.debug('Reduce Stage reduceKey:',reduceKey);
			reduceKey = reduceKey.split('_');
			var publisherName = reduceKey[0];
			log.debug('publisherName:',publisherName);
			var fileId = reduceKey[1];
			log.debug('fileId:',fileId);
			var fileObj = file.load({ id: fileId });
			var fileName = fileObj.name;
			log.debug('fileName:',fileName);

			var contentArr = context.values;
			log.debug('contentArr 1:',contentArr);
			log.debug('contentArr 1 Length :',contentArr.length);
			var rVbDate, rPublisherName, rSiteName, rSiteURL, rSmartView;
			var vbQty = 0;
			for(var cA = 0; cA < contentArr.length; cA++){
				var rContentArr = JSON.parse(contentArr[cA]);
				log.debug('rContentArr cA:',rContentArr);

				rSiteName = rContentArr.siteName;
				rSiteURL = rContentArr.siteURL;
				rVbDate = rContentArr.vbDate;
				rSmartView = rContentArr.smartView;

				log.debug('rSmartView:',Number(rSmartView));
				vbQty = vbQty + Number(rSmartView);
			}
			log.debug('Reduce Final Date to process','publisherName: '+publisherName+'; rVbDate: '+rVbDate+'; vbQty: '+vbQty);
			if(publisherName && vbQty && rVbDate){
				try{
					var validDate = checkFileDate(rVbDate);//Validate the Date in the File
					log.audit('**00 - Date in Value +'+validDate);
					if(validDate == true){
						var vendorId =  vendorSearch(publisherName); //Get Vendor Internal ID by using a Name
						if(vendorId){
							log.audit('Create VB for # '+publisherName+' & ID: '+vendorId);
							var itemId = itemSearch(publisherName); //Get Item Internal ID by using a Publisher(Vendor) Name
							log.audit('itemId',itemId);
							if(itemId){
								var monthlyFee = getMonthlyFee(publisherName,vbQty); //Get Monthly Fee from Payment Tier search
								log.audit('monthlyFee/Rate',monthlyFee);
								if(monthlyFee){
									var recCreate = createVendorBill(vendorId,itemId,monthlyFee,contentArr,vbQty,rVbDate,pubBillID); //Creating a Vendor Bill
								}else{
									errorFound = 1;
									errorMessage = 'Monthly Fee not found for Vendor: '+publisherName;
									log.error('04-errorMessage:',errorMessage);
								}
							}else{
								errorFound = 1;
								errorMessage = 'Item/Product not exist in Account for Publisher: '+publisherName;
								log.error('03-errorMessage:',errorMessage);
							}						
						}else{
							errorFound = 1;
							errorMessage = 'Vendor/Publisher Record Not exist in system';
							log.error('02-errorMessage:',errorMessage);
						}
					}else{
						errorFound = 1;
						errorMessage = 'The File contains old data, please check the Date in the file for publisher: '+publisherName;
						log.error('00-errorMessage:',errorMessage);
					}
				}catch(err){
					errorFound = 1
					errorMessage = err.message;
					log.error('01-Error in Create VB for Publisher File:',errorMessage);
				}
				//IF there is any error, capturing all the errors and passing the error details to final stage(Summarize) to send an email and also to create a error file.
				if(errorMessage && errorFound == 1){
					for(var eA = 0; eA < contentArr.length; eA++){
						var errContentArr = JSON.parse(contentArr[eA]);
						log.debug('errContentArr Error:',errContentArr);

						errSiteName = errContentArr.siteName;
						errSiteURL = errContentArr.siteURL;
						errVbDate = errContentArr.vbDate;
						errSmartView = errContentArr.smartView;

						if(eA == 0){
							errorData = '"'+errVbDate+'"'+','+'"'+publisherName+'"'+','+'"'+errSiteName+'"'+','+'"'+errSiteURL+'"'+','+'"'+errSmartView+'"'+','+'"'+errorMessage+'"'+','+'"'+fileName+'"'
						}else{
							errorData = errorData+'\r\n'+'"'+errVbDate+'"'+','+'"'+publisherName+'"'+','+'"'+errSiteName+'"'+','+'"'+errSiteURL+'"'+','+'"'+errSmartView+'"'+','+'"'+errorMessage+'"'+','+'"'+fileName+'"'
						}
					}
					log.error('**Reduce Final errorData:',errorData);
				}

				context.write({
					key: fileId,
					value : {
						'errorData': errorData,
						'errFileId': fileId,
						'errorFound': errorFound
					}
				});
			}
		}catch(e){
			log.error('Error in reduce',e);
		}

		//START: Check the Date in File is from Previous month of the execution date or not
		function checkFileDate(rVbDate){
			try{
				log.audit('rVbDate(inside checkFileDate function):',rVbDate);
				var validDate;
				//START: Get Date From File
				var fileDate;
				var dateValidate = rVbDate.indexOf('-');
				log.audit('1-dateValidate(-):',dateValidate);
				var yY, mM, dD;
				if(dateValidate != -1){
					var nDate = rVbDate.split('-');
//					yY = nDate[0];
					mM = nDate[1];
//					dD = nDate[2];
//					fileDate = yY+'/'+mM+'/'+dD;

//					log.audit('1-fileDate(Split):',fileDate);
				}else{
					var dateValidate = rVbDate.indexOf('/');
					log.audit('1-dateValidate(/):',dateValidate);
					if(dateValidate != -1){
						var nDate = rVbDate.split('/');
//						yY = nDate[2]; //mm/dd//yyyy
						mM = nDate[0];
//						dD = nDate[1];
//						fileDate = yY+'/'+mM+'/'+dD;
					}
//					log.audit('1-fileDate(No Split):',fileDate);
				}//END: Get Date From File
				log.debug('mM:',mM);
				if(mM){
					var currentMonth = new Date().getMonth();
					log.audit('currentMonth:',currentMonth);

					if(currentMonth == 0){
						if(mM == 12){
							validDate = true
						}else{
							validDate = false;
						}
					}else{
						if(currentMonth == mM){
							validDate = true
						}else{
							validDate = false;
						}
					}
				}
				return validDate;
			}catch(e){
				log.error('Error in checkFileDate function:',e);
			}
		}//END: Check the Date in File is from Previous month of the execution date or not

		// START: Get Monthly Fee from Payment Tier search
		function getMonthlyFee(publisherName,vbQty){
			try{
				log.audit("vbQty: "+vbQty);
				var monthlyFee = 0;
				var pageView, findIndex;
				var V,v1,v2 = 0;
				//Search- To find the Pricing Type
				var pricingTypeSearch = search.create({
					type: "customrecord_sn_pricing_tier_list",
					filters:
						[["custrecord_pubr_item_name_num","is",publisherName],"AND", 
						 ["isinactive","is","F"]],
						 columns:
							 [search.createColumn({name: "custrecord_pricing_type"})]
				});
				var pricingTypeSearchC = pricingTypeSearch.runPaged().count;
				log.debug("pricingTypeSearchC result count",pricingTypeSearchC);
				var priceType;
				pricingTypeSearch.run().each(function(result){
					priceType = result.getValue({name: "custrecord_pricing_type"})
					return false;
				});
				log.debug("priceType",priceType);
				if(priceType == 1 || pricingTypeSearchC > 1){ //1 = Page Views
					var customrecord_sn_pricing_tier_listSearchObj = search.create({
						type: "customrecord_sn_pricing_tier_list",
						filters:
							[
							 ["custrecord_pubr_item_name_num","is",publisherName], 
							 "AND", 
							 ["isinactive","is","F"]
							 ],
							 columns:
								 [
								  search.createColumn({name: "custrecord_sn_monthly_page_view", label: "Monthly Pageviews"}),
								  search.createColumn({name: "custrecord_sn_monthly_view", label: "Monthly Fee"}),
								  search.createColumn({name: "custrecord_cpm_rate", label: "CPM Rate"})
								  ]
					});			
					var Srch_Results = customrecord_sn_pricing_tier_listSearchObj.run().getRange({start: 0,end: 999});
					log.debug("Srch_Results", "Srch_Results length:" +Srch_Results.length);
					for(var fC = 0; fC < Srch_Results.length; fC++){
						pageView = Srch_Results[fC].getValue('custrecord_sn_monthly_page_view');
						log.debug('pageView',pageView);
						findIndex = pageView.indexOf('+');
						log.debug('findIndex',findIndex);
						if(findIndex == -1){
							pageView = pageView.split('-');
							v1 = pageView[0].trim();
							v1 = v1.replace(/,/g, '');
							v2 = pageView[1].trim();
							v2 = v2.replace(/,/g, '');
							log.debug('v1: '+v1+' &v2: '+v2);
							log.debug('vbQty:'+vbQty);

							if(v1 <= vbQty && vbQty <= v2){
								monthlyFee = Srch_Results[fC].getValue('custrecord_sn_monthly_view');
								log.debug('monthlyFee:'+monthlyFee);
								break;
							}
						}else{
							log.debug('*** + Views +***');
							pageView = pageView.split('+');
							log.debug('pageView:',pageView);
							V = pageView[0].trim();
							V = V.replace(/,/g, '');
							log.debug('V:',V);

							var tempFee = '';
							if(vbQty >= V){
								tempFee = Srch_Results[fC].getValue('custrecord_sn_monthly_view');//Monthly Fee
								log.debug('tempFee(Monthly Fee):'+tempFee);
								if(tempFee){
									monthlyFee = tempFee;
								}else{
									tempFee = Srch_Results[fC].getValue('custrecord_cpm_rate');
									log.debug('tempFee(CPM Rate):'+tempFee);
									if(tempFee){
										tempFee = (Number(tempFee)/1000)
//										log.audit("2-tempFee (CPM Rate): "+tempFee);
										tempFee = (tempFee*vbQty);
										log.audit("3-tempFee (CPM Rate): "+tempFee);
										monthlyFee = tempFee.toFixed(2);
										log.audit("4-monthlyFee (CPM Rate): "+monthlyFee);
									}
								}
								break;
							}
						}
					}
					log.debug('monthlyFee (Page View): '+monthlyFee);
				}else if(priceType == 2 && pricingTypeSearchC == 1){//2 = Flat Rate
					var flatRateSearch = search.create({
						type: "customrecord_sn_pricing_tier_list",
						filters:
							[
							 ["custrecord_pubr_item_name_num","is",publisherName], 
							 "AND", 
							 ["isinactive","is","F"]
							 ],
							 columns:
								 [
								  search.createColumn({name: "custrecord_flat_rate"})
								  ]
					});
					var flatRateSearchC = flatRateSearch.runPaged().count;
					log.debug("flatRateSearchC result count",flatRateSearchC);
					flatRateSearch.run().each(function(result){
						monthlyFee = result.getValue({name: "custrecord_flat_rate"})
						return false;
					});
					log.debug("monthlyFee (Flat Rate): "+monthlyFee);
				}else if(priceType == 3 && pricingTypeSearchC == 1){//3 = CPM
					var cpmRateSearch = search.create({
						type: "customrecord_sn_pricing_tier_list",
						filters:
							[
							 ["custrecord_pubr_item_name_num","is",publisherName], 
							 "AND", 
							 ["isinactive","is","F"]
							 ],
							 columns:
								 [
								  search.createColumn({name: "custrecord_cpm_rate"})
								  ]
					});
					var cpmRateSearchC = cpmRateSearch.runPaged().count;
					log.debug("cpmRateSearchC result count",cpmRateSearchC);
					cpmRateSearch.run().each(function(result){
						monthlyFee = result.getValue({name: "custrecord_cpm_rate"})
//						log.audit("1-monthlyFee (CPM Rate): "+monthlyFee);
						monthlyFee = (Number(monthlyFee)/1000)
//						log.audit("2-monthlyFee (CPM Rate): "+monthlyFee);
						monthlyFee = (monthlyFee*vbQty);
						log.audit("3-monthlyFee (CPM Rate): "+monthlyFee);
						monthlyFee = monthlyFee.toFixed(2);
						log.audit("4-monthlyFee (CPM Rate): "+monthlyFee);
						return false;
					});
					log.debug("monthlyFee (CPM Rate): "+monthlyFee);
					log.audit("5-monthlyFee (CPM Rate): "+monthlyFee);
				}

				//

				return monthlyFee;
			}catch(e){
				log.error('Error in Getting Monthly fee:',e);
			}
		}// END: Get Monthly Fee from Payment Tier search

		// START: Get Vendor Internal ID by using a Name
		function vendorSearch(publisherName){
			var vendorId;
			var vendorSearchObj = search.create({
				type: "vendor",
				filters:
					[
					 ["custentity_dps_snii_publisher_item_name","is",publisherName], 
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
				return false;
			});
			return vendorId;
		}// END: Get Vendor Internal ID by using a Name

		// START: Get Item Internal ID by using a Publisher(Vendor) Name
		function itemSearch(publisherName){
			var itemId;
			var noninventoryitemSearchObj = search.create({
				type: "noninventoryitem",
				filters:
					[
					 ["type","anyof","NonInvtPart"], 
					 "AND", 
					 ["name","is",publisherName], 
					 "AND", 
					 ["isinactive","is","F"]
					 ],
					 columns:
						 [
						  search.createColumn({name: "internalid", label: "Internal ID"})
						  ]
			});
			var searchResultCount = noninventoryitemSearchObj.runPaged().count;
			log.debug("noninventoryitemSearchObj result count",searchResultCount);
			noninventoryitemSearchObj.run().each(function(result){
				itemId = result.getValue('internalid');
				return false;
			});
			return itemId;
		}// END: Get Item Internal ID by using a Publisher(Vendor) Name


		// START: Creating a Vendor Bill
		function createVendorBill(vendorId,itemId,monthlyFee,contentArr,totalView,rVbDate,pubBillID){ // vbQty = totalView
			log.audit('in Create VB func (ContentArr)',contentArr);
			var arrLen = contentArr.length;
			log.audit('in Create VB func (ContentArr length)',arrLen);
//			log.audit('1-rVbDate:',rVbDate);
			log.audit('1-totalView:',totalView);

			var vendRec = record.load({type: 'vendor', id: vendorId, isDynamic: true});
			var vendCategory = vendRec.getValue({fieldId: "category"});
			log.audit('vendCategory',vendCategory);

			//********BELOW CODE NEEDS TO BE COMMENTED AFTER TESTING*************************

//			yY = 2022;
//			mM = 02;
//			dD = 6;
//			scriptExecDate = yY+'/'+mM+'/'+dD;

			//********ABOVE CODE NEEDS TO BE COMMENTED AFTER TESTING*************************

			//START: Get Previous Month last day for Transaction Date
			var pD = new Date();
			pD.setDate(0); //sets d to the last day of the previous month
			pD.setHours(0,0,0,0); //sets d time to midnight

			var pmDate = format.parse({value: pD, type: format.Type.DATE});
			log.audit('***Previous Month Date:',pmDate);				
			//END: Get Previous Month last day for Transaction Date

			// Due Date Calculation
			var scriptExecDate = new Date();
			var fDate = format.parse({value: scriptExecDate, type: format.Type.DATE});
			log.audit('Final (Bill) Date:',fDate);
			var fDueDate;

			var fDueDay;
			var fDueMonth;

			var dueD = new Date(fDate);
			log.audit('dueD:',dueD);

			var dueYear = dueD.getFullYear();
			log.audit('dueYear:',dueYear);
			var cDueMonth = dueD.getMonth();
			log.audit('cDueMonth:',cDueMonth);
			var dueDay = dueD.getDate();
			log.audit('dueDay:',dueDay);

			if(vendCategory == 5){ // 5= Monthly Publisher
//				fDueDate = new Date(dueYear, cDueMonth, fDueDay);

				var lDate = new Date(), y = lDate.getFullYear(), m = lDate.getMonth();
				var lastDay = new Date(y, m + 1, 0);
				fDueDate = format.parse({value: lastDay, type: format.Type.DATE});
				log.audit('***Current Month Last Day(Monthly):',fDueDate);

			}else if(vendCategory == 6){ //6 = Quarterly Publisher
				if(cDueMonth == 0 || cDueMonth == 1 || cDueMonth == 2){
					fDueMonth = 3;

					var lDate = new Date(), y = lDate.getFullYear(), m = fDueMonth;
					var lastDay = new Date(y, m + 1, 0);
					fDueDate = format.parse({value: lastDay, type: format.Type.DATE});
					log.audit('***Current Month Last Day(QUARTERLY 3):',fDueDate);
				}else if(cDueMonth == 3 || cDueMonth == 4 || cDueMonth == 5){
					fDueMonth = 6;

					var lDate = new Date(), y = lDate.getFullYear(), m = fDueMonth;
					var lastDay = new Date(y, m + 1, 0);
					fDueDate = format.parse({value: lastDay, type: format.Type.DATE});
					log.audit('***Current Month Last Day(QUARTERLY 6):',fDueDate);
				}else if(cDueMonth == 6 || cDueMonth == 7 || cDueMonth == 8){
					fDueMonth = 9;

					var lDate = new Date(), y = lDate.getFullYear(), m = fDueMonth;
					var lastDay = new Date(y, m + 1, 0);
					fDueDate = format.parse({value: lastDay, type: format.Type.DATE});
					log.audit('***Current Month Last Day(QUARTERLY 9):',fDueDate);
				}else if(cDueMonth == 9 || cDueMonth == 10 || cDueMonth == 11){
					fDueMonth = 0;

					var lDate = new Date(), y = lDate.getFullYear(), m = fDueMonth;
					var lastDay = new Date(y, m + 1, 0);
					fDueDate = format.parse({value: lastDay, type: format.Type.DATE});
					log.audit('***Current Month Last Day(QUARTERLY 1):',fDueDate);
				}else{
					log.error('Error in Get Month Function');
				}
			}else{
				log.error('Different Category');
			}

			log.error('fDueDate:',fDueDate);
			if(fDueDate){
				fDueDate = fDueDate;
			}else{
				log.error('No Due Date');
				fDueDate = new Date();
			}


			var vbRec = record.create({
				type: 'vendorbill',
				isDynamic: true,
				defaultValues: {
					customform: pubBillID, 
				}
			});
			vbRec.setValue('entity',vendorId);
			vbRec.setValue('department',81);
			vbRec.setValue('trandate',pmDate);
			vbRec.setValue('duedate',fDueDate);
			vbRec.setValue('memo','SV1st'); //Set Memo as SV1st
			vbRec.setValue('currency',2); //Set Currency as USD(Internal ID is 2)
			for(var vbL = 0; vbL < arrLen; vbL++){
				var rContentArray = JSON.parse(contentArr[vbL]);
				log.audit('rContentArray vbL:',rContentArray);
				var lineNum = vbRec.selectNewLine({
					sublistId: 'item'
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'item',
					value: itemId,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId:  'quantity',
					value: 0,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({//Set Tax Code as Tax free
					sublistId: 'item',
					fieldId: 'taxcode',
					value: 7,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId:  'custcol_website_name',
					value: rContentArray.siteName,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId:  'custcol_dp_pubwebsiteurl',
					value: rContentArray.siteURL,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId:  'custcol_pages_views',
					value: rContentArray.smartView,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'rate',
					value: 0,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'amount',
					value: 0,
					ignoreFieldChange: true
				});
				vbRec.setCurrentSublistValue({
					sublistId: 'item',
					fieldId: 'department',
					value: 81,
					ignoreFieldChange: true
				});
				vbRec.commitLine({sublistId: 'item'});	
			}
			//Adding Final Item Line for the rate/amount details

			var lineNum = vbRec.selectNewLine({sublistId: 'item'});
			vbRec.setCurrentSublistValue({
				sublistId: 'item',
				fieldId: 'item',
				value: itemId,
				ignoreFieldChange: true
			});
			vbRec.setCurrentSublistValue({
				sublistId: 'item',
				fieldId:  'quantity',
				value: 1,
				ignoreFieldChange: true
			});
			vbRec.setCurrentSublistValue({//Set Tax Code as Tax free
				sublistId: 'item',
				fieldId: 'taxcode',
				value: 7,
				ignoreFieldChange: true
			});
			vbRec.setCurrentSublistValue({
				sublistId: 'item',
				fieldId: 'rate',
				value: monthlyFee,
				ignoreFieldChange: true
			});
			vbRec.setCurrentSublistValue({
				sublistId: 'item',
				fieldId: 'amount',
				value: monthlyFee,
				ignoreFieldChange: true
			});
			vbRec.setCurrentSublistValue({
				sublistId: 'item',
				fieldId: 'department',
				value: 81,
				ignoreFieldChange: true
			});
			vbRec.setCurrentSublistValue({
				sublistId: 'item',
				fieldId:  'custcol_pages_views',
				value: totalView,
				ignoreFieldChange: true
			});
			vbRec.commitLine({sublistId: 'item'});

			vbRec.setValue('custbody_publisher_vendor_bill',true); // Check the "PUBLISHER VENDOR BILL?" check-box only for the Bills created for Publisher

			var vbSaved = vbRec.save(true);
			log.audit('**** vbSaved # '+vbSaved);
		}// END: Creating a Vendor Bill

	}

	/**
	 * Executes when the summarize entry point is triggered and applies to the result set.
	 *
	 * @param {Summary} summary - Holds statistics regarding the execution of a map/reduce script
	 * @since 2015.1
	 */

	//Once file has been processed the file will be moved to Processed folder from the Pending Processing Folder and if there is any error, Error file will be created and error file will move to the Error folder.
	//Status of the Process will be sent to the Key Stakeholders.
	function summarize(summary) {
		try{
			var errorFolderID;
			var processedFolderID;
			var recipientsEmail = '';
			var recipientCC = '';
			var myEnvType = JSON.stringify(runtime.envType);
			log.debug('myEnvType:',myEnvType);
			if(myEnvType === runtime.EnvType.PRODUCTION){
				errorFolderID = 734;
				processedFolderID = 732;
				recipientsEmail = 'jodi.donner@smartnews.com,isaac.teich@smartnews.com,bernie.davis@smartnews.com,aura.novembre@smartnews.com';
				recipientCC = 'amos.gitonga@smartnews.com,krishna.chouty@smartnews.com';
			}else{
				errorFolderID = 832;
				processedFolderID = 831;
				recipientsEmail = 'krishna.chouty@smartnews.com';
				recipientCC = 'krishna.chouty@smartnews.com';
			}

			var scriptObj = runtime.getCurrentScript();
			var reduceSummary = summary.reduceSummary;
			log.debug('reduceSummary',reduceSummary);

			var fileIDArr = [];
			var errorFlag = false;
			var reduceErrorData,reduceValue,processedRecord;
			var finalFileObject = [];
			var processedDataCount = 0; // To store all file Id's
			var fileId = ''; // To store all file Id's
			var errFileIdS = ''; //To store Error File Id's
			var errContent = ''; //To store Error Data's
			var errFileHeader = 'month_dt,publisher,site_name,url,smart_view,Error_Message,FileName'; 
			errContent = errFileHeader + '\r\n';
			log.debug('errContent 1',errContent);

			summary.output.iterator().each(function(key,value){
				fileId = JSON.parse(key);
				log.debug('Key fileId',fileId);
				if(fileIDArr.indexOf(fileId) == -1){
					fileIDArr.push(fileId);
				}

				reduceValue = JSON.parse(value);

				reduceErrorData = reduceValue.errorData;
				log.debug('reduceErrorData',reduceErrorData);
				if(reduceErrorData){
					errContent = errContent + reduceErrorData + '\r\n'; 
					errFileIdS = reduceValue.errFileId;
					log.debug('errFileIdS',errFileIdS);

					errorFlag =  true;
				}
				return true;
			});

			//START: Create Error File
			var errFileDownloaded = '';
			var errFileName;
			if(errContent && errFileIdS && errorFlag){

				var OrgFileObj = file.load({ id: errFileIdS });
				var OrignalFileName = OrgFileObj.name;
				log.debug('OrignalFileName:',OrignalFileName);
				OrignalFileName = OrignalFileName.split('.');
				OrignalFileName = OrignalFileName[0];
				OrignalFileName = OrignalFileName+'_ErrorFile.csv';
				log.debug('Updated Original File Name:',OrignalFileName);

				errFileName = OrignalFileName;

				var errFileObj = file.create({
					name: errFileName,
					fileType: file.Type.CSV,
					contents: errContent,
					description: 'This file contains details of Monthly Payment for Publisher Error',
					folder: errorFolderID
				});
				errFileDownloaded = errFileObj.save();
				log.debug('errFileDownloaded:',errFileDownloaded);
			}//END: Create Error File

//			/*
			if(fileIDArr.length > 0){
				//START: Move File to Processed Folder
				var OrigFileName = '';
				for(var fA = 0; fA < fileIDArr.length; fA++){
					var fileObj = file.load({ id: fileIDArr[fA] });
					finalFileObject.push(fileObj);
					OrigFileName = fileObj.name;

					fileObj.folder = processedFolderID;
					var fileId = fileObj.save();
					log.debug('File Moved:',fileId);
				}//END: Move File to Processed Folder

				//START: Send Email's to Key stake-holder

				var emailArr = [];
				var emailCC = [];

				var senderId = 204549; //Sender is Krishna, Employees Internal ID needs to be updated

				//Only 10 person we can add in the recipientEMail and CC
				//FOR PRODUCTION
//				var recipientsEmail = 'jodi.donner@smartnews.com,isaac.teich@smartnews.com,bernie.davis@smartnews.com,aura.novembre@smartnews.com';
//				var recipientCC = 'amos.gitonga@smartnews.com,krishna.chouty@smartnews.com';

				//FOR SB2
//				var recipientsEmail = 'krishna.chouty@smartnews.com';
//				var recipientCC = 'krishna.chouty@smartnews.com';

				emailArr.push(recipientsEmail);
				emailCC.push(recipientCC);
				var emailBody = '';
				var emailSubject = 'Monthly Payment Publisher file Processed status';

				if(errFileDownloaded){
					log.debug('Error file Exist');
					emailBody = 'Hi,'+'\r\n'+'\r\n'+'Monthly Payment Publisher File has been Processed.'+'\r\n'+' Due to some error all records are not processed. To find the error details refer the attachment '+'\r\n'+' File Name: "'+OrigFileName+'"'  +'\r\n'+'Error File Name: "'+errFileName+'"'+'\r\n'+'\r\n'+'Thanks,'+'\r\n'+'Admin.'; //If Error then this body will be added in Email
					fileObject = file.load({
						id: errFileDownloaded
					});	
					finalFileObject.push(fileObject);
				}else{
					log.debug('Error file Not-Exist');
					emailBody = 'Hi,'+'\r\n'+'\r\n'+'Monthly Payment Publisher File has been Processed Successfully.'+'\r\n'+ 'File Name: "'+OrigFileName+'"' +'\r\n'+'\r\n'+'\r\n'+'Thanks,'+'\r\n'+'Admin.';//If No Error then this body will be added in Email
				}
				log.debug('finalFileObject:',finalFileObject);     //searchResultCount
				email.send({
					author: senderId,
					recipients: emailArr,
					cc: emailCC,
					subject: emailSubject,
					body: emailBody,
					attachments: finalFileObject
				});
			}
//			*/
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
