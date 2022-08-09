/**
 * @NApiVersion 2.x
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
define(['N/search','N/file'],

		function(search,file) {

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
	function getInputData() {
		try{
			var finalContent = '';
			var startIndex = 0;
			var endIndex;
			var content='';
			var accountMasterSearch = search.load({
				id: 'customsearch_account_master_for_snowflak'
			});
			var searchResult = accountMasterSearch.run();
			var searchResultCount = accountMasterSearch.runPaged().count;
			log.debug('searchResultCount',searchResultCount);
			var endLine = 0;

			var result = searchResult.getRange({start : 0, end : 900});
			var resultLen = result.length;
			log.debug('resultLen',resultLen);

			if(resultLen > 0){

				var column = searchResult.columns;
				do{
					endIndex = startIndex+1000;
					//Taking 1000 records from Saved Search
					var resultSet = searchResult.getRange({start : startIndex, end : endIndex});
					var len = resultSet.length;
					log.debug('len',len);
					log.debug('endLine',endLine);

					for(var i=0 ; i<len ; i++){
						endLine++;
						log.debug('In For Loop');
						for(var j=0; j<column.length ; j++){
							if(j==3){
								var textResult = resultSet[i].getText(resultSet[i].columns[j]);
								if(textResult == '' || textResult == null || textResult == undefined)
								{
									content = content + ',' + '';
								}
								else
								{
									textResult = textResult.replace(/,/g, '');
									content = content + ',' + textResult.trim();

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
									else
										content = content +',' + valueResult.trim();
								}
							}

						}//end of j for loop
						if(searchResultCount!=endLine)
							content = content+'\r\n'
					}//end of i for loop
					startIndex = endIndex;
				}while(len > 0);
			}
			if(content){
				var fileHeader = 'Number,Name,Display_Name,Account_Type';
				
				finalContent = fileHeader + '\r\n';
				finalContent = content;
				
				var fileObj = file.create({
					name: 'Account_Master.csv',
					fileType: file.Type.CSV,
					contents: finalContent,
					description: 'This contains details of Account Master',
					folder: 4635
				});
				var fileDownloaded = fileObj.save();
				log.debug('fileDownloaded:',fileDownloaded);
				
			}
		}catch(e){
			log.debug('Error in getInputData',e);
		}
	}

	/**
	 * Executes when the map entry point is triggered and applies to each key/value pair.
	 *
	 * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
	 * @since 2015.1
	 */
	function map(context) {
		try{

		}catch(e){
			log.debug('Error in map',e);
		}
	}

	/**
	 * Executes when the reduce entry point is triggered and applies to each group.
	 *
	 * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
	 * @since 2015.1
	 */
	function reduce(context) {
		try{

		}catch(e){
			log.debug('Error in reduce',e);
		}
	}


	/**
	 * Executes when the summarize entry point is triggered and applies to the result set.
	 *
	 * @param {Summary} summary - Holds statistics regarding the execution of a map/reduce script
	 * @since 2015.1
	 */
	function summarize(summary) {
		try{

		}catch(e){
			log.debug('Error in summarize',e);
		}
	}

	return {
		getInputData: getInputData,
		map: map,
		reduce: reduce,
		summarize: summarize
	};

});
