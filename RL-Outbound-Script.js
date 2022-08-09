/**
 * @NApiVersion 2.x
 * @NScriptType Restlet
 * @NModuleScope SameAccount
 */

var output ='';

define(['N/search'],

		function(search) {

	function doPost(requestBody) {
		var output;
		log.debug('<===========START===============>',requestBody);
		if(requestBody == '')
		{
			log.error('Error','Data Not found');
			output = getFailureObj('Data Not found');
			return output;  	
		}	
		else
		{
			log.debug('requestBody after parse',requestBody);
			var fileName = requestBody.fileName;
			log.debug('fileName',fileName);

			if(fileName){
				output = searchAccountDetails(fileName, search);				
				log.debug('Output in main',output);
			}else{
				log.error('Error','Send Data in JSON Format');
				output = getFailureObjJson('Send Data in JSON Format');
				return output; 
			}
			return JSON.stringify(output);
		} 
	}

	/* Respond back in case of any error */
	function getFailureObj(errorMsg){
		return {"StatusCode":"FAILURE","Msg": errorMsg};
	}

	function getFailureObjJson(errorMsg){
		return {"StatusCode":"FAILURE","Msg": errorMsg};
	}

	//START: Search to get Item Details.
	function searchAccountDetails(fileName, search){
		var startIndex = 0;
		var endIndex;
		var content='';
		var fileHeader = '';
		if(fileName == 'Account_Master'){
			var searchObj = search.load({
				id: 'customsearch_account_master_for_snowflak'
			});
			var searchResult = searchObj.run();
			var searchResultCount = searchObj.runPaged().count;
			log.debug('searchResultCount',searchResultCount);
			var endLine = 0;

			var result = searchResult.getRange({start : 0, end : 900});
			var resultLen = result.length;
			log.debug('resultLen',resultLen);

			if(resultLen > 0){
				var column = searchResult.columns;

				for(var i=0 ; i<column.length; i++)
				{
//					log.debug('Inside fileHeader:',column);
					if(i==0)
					{
						fileHeader = fileHeader + column[i].label;
					}
					else
					{
						fileHeader = fileHeader+ ',' + column[i].label;
					}
				}
				content = fileHeader + '\r\n';
//				log.debug('Content:',content);
				do{
					endIndex = startIndex+1000;
					//Taking 1000 records from Saved Search
					var resultSet = searchResult.getRange({start : startIndex, end : endIndex});
					var len = resultSet.length;
					log.debug('len',len);
					log.debug('endLine',endLine);

					for(var i=0 ; i<len ; i++){
						endLine++;
						//Fetching column data
						for(var j=0; j<column.length ; j++){
							if(j == 0){
								var valueResult = resultSet[i].getValue(resultSet[i].columns[j]);

								if(valueResult == '' || valueResult == null || valueResult == undefined)
								{
									content = content + '';
								}
								else{
									valueResult = valueResult.replace(/,/g, '');
									content = content + valueResult.trim();
								}
							}else{
								var valueResult = resultSet[i].getValue(resultSet[i].columns[j]);

								if(valueResult == '' || valueResult == null || valueResult == undefined)
								{
									content = content  + ',' + '';
								}
								else{
									valueResult = valueResult.replace(/,/g, '');
									content = content +',' + valueResult.trim();
								}
							}
//							}
						}//end of j for loop
						if(searchResultCount!=endLine)
							content = content+'\r\n';
					}//end of i for loop
					startIndex = endIndex;
				}while( len > 0);
			}

			output = content;

			return output;
		}else if(fileName == 'Transaction_List'){
			log.debug('In transactionList Function');
			//Original Search ID: customsearch_sn_je_list_for_sf_intergrat
			var searchObj = search.load({
				id: 'customsearch_sn_je_list_for_sf_intergr_s'
			});
			var searchResult = searchObj.run();
			var searchResultCount = searchObj.runPaged().count;
			log.debug('Search Count of Transaction List',searchResultCount);
			var endLine = 0;

			var result = searchResult.getRange({start : 0, end : 900});
			var resultLen = result.length;
			log.debug('resultLen',resultLen);

			if(resultLen > 0){
				var column = searchResult.columns;

				for(var i=0 ; i<column.length; i++)
				{
//					log.debug('Inside fileHeader:',column);
					if(i==0)
					{
						fileHeader = fileHeader + column[i].label;
					}
					else
					{
						fileHeader = fileHeader+ ',' + column[i].label;
					}
				}
				content = fileHeader + '\r\n';
//				log.debug('Content:',content);
				do{
					endIndex = startIndex+1000;
					//Taking 1000 records from Saved Search
					var resultSet = searchResult.getRange({start : startIndex, end : endIndex});
					var len = resultSet.length;
					log.debug('len',len);
					log.debug('endLine',endLine);

					for(var i=0 ; i<len ; i++){
						endLine++;
						//Fetching column data
						for(var j=0; j<column.length ; j++){
							if(j == 1 || j == 2 || j == 8 || j == 10 || j == 11 || j == 13 || j == 17){
								var textResult = resultSet[i].getText(resultSet[i].columns[j]);

								if(textResult == '' || textResult == null || textResult == undefined)
								{
									content = content  + ',' + '';
								}
								else{
									textResult = textResult.replace(/,/g, '');
									content = content +',' + textResult.trim();
								}
							}else{
								if(j == 0){
									var valueResult = resultSet[i].getValue(resultSet[i].columns[j]);

									if(valueResult == '' || valueResult == null || valueResult == undefined)
									{
										content = content + '';
									}
									else{
										valueResult = valueResult.replace(/,/g, '');
										content = content + valueResult.trim();
									}
								}else{
									var valueResult = resultSet[i].getValue(resultSet[i].columns[j]);

									if(valueResult == '' || valueResult == null || valueResult == undefined)
									{
										content = content  + ',' + '';
									}
									else{
										valueResult = valueResult.replace(/,/g, '');
										content = content +',' + valueResult.trim();
									}
								}
							}

						}//end of j for loop
						if(searchResultCount!=endLine)
							content = content+'\r\n';
					}//end of i for loop
					startIndex = endIndex;
				}while( len > 0);
			}

			output = content;

			return output;
		}else if(fileName == 'Publisher_Url'){
			log.debug('In Publisher URL Latest Report Function');
			//Original Search ID: customsearch_sn_je_list_for_sf_intergrat
			var searchObj = search.load({
				id: 'customsearch_publ_url_latest_report'
			});
			var searchResult = searchObj.run();
			var searchResultCount = searchObj.runPaged().count;
			log.debug('Search Count of Transaction List',searchResultCount);
			var endLine = 0;

			var result = searchResult.getRange({start : 0, end : 900});
			var resultLen = result.length;
			log.debug('resultLen',resultLen);

			if(resultLen > 0){
				var column = searchResult.columns;

				for(var i=0 ; i<column.length; i++)
				{
//					log.debug('Inside fileHeader:',column);
					if(i==0)
					{
						fileHeader = fileHeader + column[i].label;
					}
					else
					{
						fileHeader = fileHeader+ ',' + column[i].label;
					}
				}
				content = fileHeader + '\r\n';
//				log.debug('Content:',content);
				do{
					endIndex = startIndex+1000;
					//Taking 1000 records from Saved Search
					var resultSet = searchResult.getRange({start : startIndex, end : endIndex});
					var len = resultSet.length;
					log.debug('len',len);
					log.debug('endLine',endLine);

					for(var i=0 ; i<len ; i++){
						endLine++;
						//Fetching column data
						for(var j=0; j<column.length ; j++){
							if(j == 0){
								var valueResult = resultSet[i].getText(resultSet[i].columns[j]);

								if(valueResult == '' || valueResult == null || valueResult == undefined)
								{
									content = content + '';
								}
								else{
									valueResult = valueResult.replace(/,/g, '');
									content = content + valueResult.trim();
								}
							}else{
								var valueResult = resultSet[i].getValue(resultSet[i].columns[j]);

								if(valueResult == '' || valueResult == null || valueResult == undefined)
								{
									content = content  + ',' + '';
								}
								else{
									valueResult = valueResult.replace(/,/g, '');
									content = content +',' + valueResult.trim();
								}
							}
						}//end of j for loop
						if(searchResultCount!=endLine)
							content = content+'\r\n';
					}//end of i for loop
					startIndex = endIndex;
				}while( len > 0);
			}

			output = content;

			return output;

		}else{
			output = 'Request is not a Valid request.';
			return output;
		}

	}

	return {
		post: doPost
	};

});
