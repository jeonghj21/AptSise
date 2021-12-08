let gRegionsMap = {}; //
let gCookie_Region = "latest_region";
let loadingApt = false;
let BASE_URL;

let YAXE_POSITION = { 
	LEFT : 1,
	RIGHT : 2
};

const CHART_KEY_SEPARATOR = "$";

class ChartInfo {
	
	constructor(params, data) {
		this._params = params;
		this._data = data;
	}

	get params() {
		return this._params;
	}

	get data() {
		return this._data;
	}

	get apt() {
		return this._params['apt'];
	}

	set apt(newValue) {
		ChartManager.sChartParams['apt'] = newValue;
		this._params['apt'] = newValue;
	}

	set aptName(newValue) {
		this._params['aptName'] = newValue;
	}

	get region_key() {
		return this._params['region_key'];
	}

	set region_key(newValue) {
		ChartManager.sChartParams['region_key'] = newValue;
		this._params['region_key'] = newValue;
	}

	get from_ym() {
		return this._params['from_ym'];
	}

	set from_ym(newValue) {
		ChartManager.sChartParams['from_ym'] = newValue;
		this._params['from_ym'] = newValue;
	}

	get to_ym() {
		return this._params['to_ym'];
	}

	set to_ym(newValue) {
		ChartManager.sChartParams['to_ym'] = newValue;
		this._params['to_ym'] = newValue;
	}

	get priceGubun() {
		return PriceGubun.getPriceGubun(this._params);
	}

	set priceGubun(gubun) {

		ChartManager.sChartParams['price_gubun'] = gubun;
		this._params['price_gubun'] = gubun;
		
	}

	get base_ym() {
		return this._params['base_ym'];
	}

	set base_ym(newValue) {
		ChartManager.sChartParams['base_ym'] = newValue;
		this._params['base_ym'] = newValue;
	}

	get years() {
		return this._params['years'];
	}

	set years(newValue) {
		ChartManager.sChartParams['years'] = newValue;
		this._params['years'] = newValue;
	}

	get page() {
		return this._params['page'];
	}

	set page(newValue) {
		this._params['page'] = newValue;
	}

	get order_by() {
		return this._params['orderby'];
	}

	set order_by(newValue) {
		this._params['orderby'] = newValue;
	}

	addParam(key, value) {
		this._params[key] = value;
	}

}

class ChartWrapper {
	
	static sMainChart = null;

	static addRightYAxe(label, id) {

		if (ChartWrapper.sMainChart['options']['scales']['yAxes'].length > 1)
			ChartWrapper.sMainChart['options']['scales']['yAxes'].pop();

		ChartWrapper.sMainChart['options']['scales']['yAxes'].push({
			id: id,
			type: 'linear',
			position: 'right',
			scaleLabel: {
				display: true,
				labelString: label
			},
			ticks: {
				min: 0
			}
		});

	}
	
	static setYLabel(label) {
		ChartWrapper.sMainChart['options']['scales']['yAxes'][0].scaleLabel.labelString = label;
	}

	static setLegendFunction(legendCallbackFunc) {

		ChartWrapper.sMainChart.options['legend'] = false;
		ChartWrapper.sMainChart.options['legendCallback'] = legendCallbackFunc;

	}

	static chartLegend() {
		return ChartWrapper.sMainChart.generateLegend();
	}

	static getChartCount() {
		if (ChartWrapper.sMainChart == null)
			return 0;
		return ChartWrapper.sMainChart.data.datasets.length;
	}

	static getChartLabel(i) {
		return ChartWrapper.sMainChart.data.datasets[i].label;
	}

	static getChartData(i) {
		return ChartWrapper.sMainChart.data.datasets[i].data;
	}

	static getChartLabels() {
		return ChartWrapper.sMainChart.data.labels;
	}

	static getChartBgcolor(index) {
		return ChartWrapper.sMainChart.data.datasets[index].backgroundColor ? 
				ChartWrapper.sMainChart.data.datasets[index].backgroundColor : 
				ChartWrapper.sMainChart.options.defaultColor;
	}

	static setChartThick(index, bThick) {
		let meta = ChartWrapper.sMainChart.getDatasetMeta(index);
		if (bThick) {
			ChartWrapper.sMainChart.data.datasets[index].borderWidth 
				= 2 + meta.controller._cachedDataOpts.borderWidth;
			ChartWrapper.sMainChart.data.datasets[index].oldColor = meta.controller._cachedDataOpts.borderColor;
			ChartWrapper.sMainChart.data.datasets[index].borderColor = ChartWrapper.sMainChart.data.datasets[index].thickColor;
		} else {
			ChartWrapper.sMainChart.data.datasets[index].borderWidth 
				= (meta.bar ? Chart.defaults.global.elements.rectangle.borderWidth : Chart.defaults.global.elements.line.borderWidth);
			ChartWrapper.sMainChart.data.datasets[index].borderColor = ChartWrapper.sMainChart.data.datasets[index].oldColor;
		}

		ChartWrapper.sMainChart.update();
	}

	static getDataIndicesOfEvent(evt) {
	
		let indices = [];
		let activePoint = ChartWrapper.sMainChart.getElementAtEvent(evt);

		// make sure click was on an actual point
		if (activePoint.length == 0)
			return indices;

		indices.push(activePoint[0]._datasetIndex);
		indices.push(activePoint[0]._index);

		return indices;

	}

	static createChart(chartInfo, data, yLabel, tooltipFunc) {

		ChartWrapper.clearChart();

		let ctx = document.getElementById("myChart").getContext("2d");
		ChartWrapper.sMainChart = new Chart (ctx, {
			type: chartInfo.chart_shape,
			data: {
				labels : data['labels'],
				datasets : []
			},
			options : {
				scales: {
					yAxes: [{
						id: 'A',
						type: 'linear',
						scaleLabel: {
							display: true,
							labelString: yLabel
						},
						position: 'left'
					}]
				}
			}
		});
		ChartWrapper.sMainChart.config.options.scales.yAxes[0].scaleLabel.labelString = yLabel;

		document.getElementById("myChart").onclick = chartClickEventHandler;

		if (tooltipFunc) {
			ChartWrapper.sMainChart.options.tooltips.callbacks = {
       			beforeBody: tooltipFunc
			};
		}

		ChartWrapper.sMainChart.update();
	}

	static chartComplete(legendFunc = null) {
		if (legendFunc) {
			ChartWrapper.setLegendFunction(legendFunc);
			document.getElementById('chartLegend').innerHTML = ChartWrapper.chartLegend(); 
		}
		ChartWrapper.sMainChart.update();
	}

	static removeChart(key) {

		// 기본 차트에 이동평균 또는 거래량 차트가 추가되어 있으면 그것도 지운다
		for(i = 0; i < ChartWrapper.getChartCount(); i++) {
			if (ChartWrapper.sMainChart.data.datasets[i].label.startsWith(key)) {
				ChartWrapper.sMainChart.data.datasets.splice(i, 1);
				i--;
			}
		}
		ChartWrapper.sMainChart.update();
	}

	static clearChart() {

		if (ChartWrapper.sMainChart != null) {
			ChartWrapper.sMainChart.destroy();
			ChartWrapper.sMainChart = null;
		}

	}

	static showChart(index, bShow) {
		ChartWrapper.sMainChart.data.datasets[index].hidden = !bShow;
		ChartWrapper.sMainChart.update();
	}

	static emptyChart() {
		while(ChartWrapper.sMainChart['data']['datasets'].length > 0)
			ChartWrapper.sMainChart['data']['datasets'].pop();
	}

	static makeChartDataset(chartType, title, yAxisID, data, color, additionalParams) {

		let dataset = {
			type: chartType,
			label: title,
			yAxisID: yAxisID,
			data: data,
			spanGaps: true
		};
		if (color)
			dataset.thickColor = color;

		if (chartType == 'line') {
			dataset.fill = false;
		}

		if (additionalParams)
			Object.keys(additionalParams).forEach(function(key) { 
					dataset[key] = additionalParams[key]; 
				});

		ChartWrapper.sMainChart['data']['datasets'].push(dataset);

		ChartWrapper.sMainChart.update();
	}

	static setXAxeForDualBar() {
	
		ChartWrapper.sMainChart.options.scales['xAxes'][0]['stacked'] = true;
		ChartWrapper.sMainChart.options.scales['xAxes'][0]['offset'] = true;
		ChartWrapper.sMainChart.options.scales['xAxes'][0]['gridLines'] = { offsetGridLines: true };
		ChartWrapper.sMainChart.options.scales['xAxes'].push({
	        stacked: true,
			display: false,
	    	id: "X2",
		    gridLines: {
			    offsetGridLines: true
			},
	    	offset: true
   		});
	}

	static existsChart() {
		return ChartWrapper.sMainChart != null;
	}

}

class ChartManager {

	static sChartTypes = [];
	static sChartParams = null;
	static sDrawOptions = {};
	static sChartInfos = new Map();

	constructor(title, chart_shape, base_url, need_params) {
		this._title = title;
		this._chart_shape = chart_shape;
		this._base_url = base_url;
		this._need_params = need_params;

		this._index = ChartManager.sChartTypes.length;

		ChartManager.sChartTypes.push(this);
	}

	get chart_shape() {
		return this._chart_shape;
	}

	title(params) {
		let title = "[" + this._title + "]";
		let chartType = ChartManager.getChartType(params);
		if (chartType == CHART_TYPE.TIME_SERIES_APT)
			title += getAptTitle(params);
		else
			title += getRegionTitle(params);
		title += makeChartConditionTitle(params, this._need_params);
		return title;
	}

	url(params) {
		return getChartURL(this._base_url, params);
	}

	static getChartType(params) {
		let index = params['chart_type'];
		const errorMsg = "index in params is invalid";
		console.assert(Number.isInteger(index) && index >= 0 && index < ChartManager.sChartTypes.length, 
			{index: params['index'], errorMsg: errorMsg});

		return ChartManager.sChartTypes[index];
	}

	static getChartTypeForChart(chartType) {

		let theChartManager = null;
		ChartManager.sChartTypes.forEach(function(aChartManager) {
			if (aChartManager == chartType) {
				theChartManager = aChartManager;
			}
		});
		const errorMsg = "unknown chartType";
		console.assert(!theChartManager, {errorMsg: errorMsg});

		return theChartManager;

	}

	static getCurChartType() {
		let chartHistSel = document.getElementById("chartHist");
		let curChart = chartHistSel.options[chartHistSel.selectedIndex];
		let key = curChart.value.split(CHART_KEY_SEPARATOR)[0];
		let chartData = ChartManager.sChartInfos.get(key);
		let index = chartData.params['chart_type'];
		const errorMsg = "index in params is invalid";
		console.assert(Number.isInteger(index) && index >= 0 && index < ChartManager.sChartTypes.length, 
			{index: params['index'], errorMsg: errorMsg});
	
		return ChartManager.sChartTypes[index];
	}

	static setChartType(params, chartType) {
		let i = 0;
		for(; i < ChartManager.sChartTypes.length; i++) {
			if (ChartManager.sChartTypes[i] == chartType) {
				break;
			}
		};
		const errorMsg = "unknown chartType";
		console.assert(i >= 0 && i < ChartManager.sChartTypes.length, {errorMsg: errorMsg});
		params['chart_type'] = i;

		return ChartManager.sChartTypes[i];
	}

	static addChartHistory(title) {
		let sel = document.getElementById("chartHist");
		if (ChartManager.isDrawOverlap() && ChartWrapper.getChartCount() > 0) {
    		let option = sel.options[sel.selectedIndex];
			option.value += CHART_KEY_SEPARATOR + title;
    		let caption = "월별추이[복합]";
			let key_arr = option.value.split(CHART_KEY_SEPARATOR);
			key_arr.forEach(function(key) {
				let chartInfo = ChartManager.getChartInfo(key);
				caption += "[";
				if (chartInfo.chartType == CHART_TYPE.TIME_SERIES_APT)
					caption += getAptTitle(chartInfo.params);
				else
					caption += getRegionTitle(chartInfo.params);
				caption += "]";
			});
    		option.innerHTML = caption;
		} else {

			// 기존에 있던 차트면 select에서 삭제
			let chart = ChartManager.sChartInfos.get(title);
			if (chart) {
				for(let i = 0; i < sel.options.length; i++) {
					if (sel.options[i].value == title) {
						sel.remove(i);
						break;
					}
				}
			}
			let option = document.createElement("option");
			option.value = title;
			option.innerHTML = title;
			option.selected = true;
			sel.appendChild(option);
		}
	}

	static initDrawOptionsMap() {

		let params = {};

		document.querySelectorAll('#draw_options input').forEach(function(input, index) {
			let key = input.id;
			params[key] = input;
		});

		ChartManager.sDrawOptions = { params: params };

		Object.keys(params).forEach(function(key) {

			let item = params[key];
			let input = item;

			Object.defineProperty(ChartManager.sDrawOptions, key, {
				enumerable: true,
				get: function () { 
					let input = this.params[key];
					return input.checked;
				},
				set: function(bool) {
					let item = this.params[key];
					item.checked = (bool == true);
				}
			});
		});
		Object.defineProperty(ChartManager.sDrawOptions, "checked", {
			set: function(bool) {
				Object.values(this.params).forEach(function(item) {
					item.checked = bool;
				});
			}
		});
		Object.defineProperty(ChartManager.sDrawOptions, "disabled", {
			set: function(bool) {
				Object.values(this.params).forEach(function(item) {
					item.disabled = bool;
				});
			}
		});

	}

	static initChartParams() {
		let params = {};

		let items = document.querySelectorAll('#chart_conditions input, #chart_conditions select, #chart_conditions ul');
		items.forEach(function(input, index) {
			let key = input.id;
			if (params[key]) {
				let tmp = params[key];
				if (!Array.isArray(tmp)) {
					params[key] = [ tmp ];
					params[key].value_len = input.value.length;
				}
				params[key].push(input);
			} else
				params[key] = input;
		});

		params['orderby'] = '';
		params['page'] = '';
		params['ym'] = '';
		params['region_key'] = '0000000000';

		let map = { params: params };

		Object.keys(params).forEach(function(key) {

			let item = params[key];
			let input = item;
			if (Array.isArray(item))
				input = item[0];

			Object.defineProperty(map, key,
			{ 
				enumerable: true,
				get: function () { 
					return getValueOfParam(this.params, key);
				}, 
				set: function(newValue) {
					setValueOfParam(this.params, key, newValue);
				}
			});
		});

		return map;

	}

	static initChartHistory() {
	
		const sel = document.getElementById('chartHist');

		sel.addEventListener('change', (event) => {
			let key_arr = sel.options[sel.selectedIndex].value.split(CHART_KEY_SEPARATOR);
			let chart = ChartManager.sChartInfos.get(key_arr[0]);
			switch(ChartManager.getChartType(chart.params)) {
				case CHART_TYPE.TIME_SERIES_REGION:
				case CHART_TYPE.TIME_SERIES_APT:
					ChartManager.drawOverlap = (key_arr.length > 1);
					key_arr.forEach(function(key) {
						let chartInfo = ChartManager.sChartInfos.get(key);
						drawTimeSeriesChart(key, chartInfo.data, chartInfo.params);
					});
					break;
				case CHART_TYPE.RANK_REGION:
				case CHART_TYPE.RANK_APT:
					drawRankChartBase(key_arr[0], chart.data, chart.params);
					break;
				case CHART_TYPE.COMPARE:
					drawCompareChart(key_arr[0], chart.data, chart.params);
					break;
			}
		});
	
	}

	static init() {
		
		ChartManager.initDrawOptionsMap();

		ChartManager.sChartParams = ChartManager.initChartParams();

		ChartManager.initChartHistory();
	}

	static getChartParams() {

		return ChartManager.sChartParams;
	}

	static createChart(title, data, params, yLabel, tooltipFunc) {

		ChartManager.addChart(title, params, data);

		ChartManager.addChartHistory(title);

		resetChartColor();

		let chartType = ChartManager.getChartType(params);

		if (chartType == CHART_TYPE.RANK_REGION || chartType == CHART_TYPE.RANK_APT
			|| !ChartManager.isDrawOverlap() || !ChartWrapper.existsChart())
			ChartWrapper.createChart(chartType, data, yLabel, tooltipFunc);

		if (chartType == CHART_TYPE.TIME_SERIES_REGION || chartType == CHART_TYPE.TIME_SERIES_APT) {
			ChartManager.sDrawOptions.disabled = false;
		} else {
			ChartManager.sDrawOptions.checked = false;
			ChartManager.sDrawOptions.disabled = true;
		}
	}

	static removeChart(key) {

		let sel = document.getElementById("chartHist");
		let opt = sel.options[sel.selectedIndex];
		let arr = opt.value.split(CHART_KEY_SEPARATOR);
		if (arr.length > 1) {
			opt.value = "";
			for (let i = 0; i < arr.length; i++) {
				if (arr[i] == key) {
					arr.splice(i, 1);
					i--;
				} else {
					if (i > 0)
						opt.value += CHART_KEY_SEPARATOR;
					opt.value += arr[i];
				}
			}
			if (arr.length == 1) {
				opt.textContent = opt.value;
			}
		} else {
			sel.removeChild(opt);
			if (sel.options.length > 0) {
				let evt = document.createEvent("HTMLEvents");
				evt.initEvent("change", false, true);
				sel.dispatchEvent(evt);
			}
		}
		ChartManager.sChartInfos.delete(key);
		ChartWrapper.removeChart(key);

		document.getElementById('chartLegend').innerHTML = ChartWrapper.chartLegend(); 
	}

	static isDrawOverlap() {
		return ChartManager.sDrawOptions['draw_overlap'];
	}

	static set drawOverlap(newValue) {
		ChartManager.sDrawOptions['draw_overlap'] = newValue;
	}

	static isDrawRelative() {
		return ChartManager.sDrawOptions['draw_relative'];
	}

	static isDrawVolume() {
		return ChartManager.sDrawOptions['draw_volume'];
	}

	static isDrawMA() {
		return ChartManager.sDrawOptions['draw_ma'];
	}

	static getChartKeys() {
		return ChartManager.sChartInfos.keys();
	}

	static getChartInfo(key, startIndex) {

		if (startIndex < 0)
			startIndex = ChartWrapper.getChartCount() - 1;

		let chartInfo = ChartManager.sChartInfos.get(key);
		if (chartInfo == null) {
			for(let i = startIndex; i >= 0; i--) {
				if (key.startsWith(ChartWrapper.getChartLabel(i))) {
					chartInfo = ChartManager.sChartInfos.get(ChartWrapper.getChartLabel(i));
					if (chartInfo) {
						break;
					}
				}
			}
		}

		return chartInfo;
	}
		
	static getCurChartInfo() {
		
		// return first chart
		let i = 0;
		let chartInfo = null;
		while(!chartInfo && i < ChartWrapper.getChartCount()) {
			let key = ChartWrapper.getChartLabel(i);
			chartInfo = ChartManager.sChartInfos.get(key);
			i++;
		}

		console.assert(chartInfo, "chart data invalid");

		return chartInfo;
	}

	static addChart(key, params, data) {
		
		let copy_params = deep_copy_params(params);

		let tmp = $.extend(true, {}, data);
		
		ChartManager.sChartInfos.set(key, new ChartInfo(copy_params, tmp));
	
	}

	static existsInCurChart(title) {
		for (let i = 0; i < ChartWrapper.sMainChart.data.datasets.length; i++) {
			if (ChartWrapper.sMainChart.data.datasets[i].label == title)
				return true;
		}

		return false;
	}

}

const CHART_TYPE = {
	TIME_SERIES_REGION: 
		new ChartManager("월별 추이", "line", "getSaleStat", 
				['region_key', 'from_ym', 'to_ym', 'danji', 'ages', 'age_sign', 'area_type']),
	TIME_SERIES_APT: 
		new ChartManager("월별 추이", "line", "getSale", 
				['apt', 'from_ym', 'to_ym', 'area_type']),
	RANK_REGION: 
		new ChartManager("지역별 순위", "line", "getRankByRegion", 
				['region_key', 'base_ym', 'years', 'danji', 'ages', 'age_sign', 'area_type']),
	RANK_APT: 
		new ChartManager("아파트별 순위", "line", "getRankByApt", 
				['apt', 'base_ym', 'years', 'danji', 'ages', 'age_sign', 'area_type'])
};

function deep_copy_params(params) {
	let copy_params = {};
	for (let key in params) {
		let item = params[key];
		if (typeof(item) != "object")
			copy_params[key] = item;
	}

	return copy_params;
}


class OrderBy {
	constructor(var_name, title) {
		this._var_name = var_name;
		this._title = title;
	}

	get var_name() {
		return this._var_name;
	}

	get title() {
		return this._title;
	}
}

class PriceGubun {
	constructor(title, price_name, ma_name, rate_name) {
		this._title = title;
		this._price_name = price_name;
		this._ma_name = ma_name;
		this._rate_name = rate_name;

		this._order_by_arr = [ new OrderBy(rate_name, '상승율순 조회')
							 , new OrderBy(price_name, '기준가격순 조회')
							 , new OrderBy('name', '가나다순 조회') ];
	}

	get title() {
		return this._title;
	}

	get price_name() {
		return this._price_name;
	}

	get ma_name() {
		return this._ma_name;
	}

	get rate_name() {
		return this._rate_name;
	}

	get before_price_name() {
		return "before_" + this._price_name;
	}

	static cPriceGubun = [ new PriceGubun('평단가(만원)', 'uprice', 'uma', 'urate'),
						   new PriceGubun('매매가(만원)', 'price', 'ma', 'rate') ];

	static getPriceGubun(params) {
		let nPriceGubun = -1;

		let target = '';
		if (typeof params == "object")
			target = params['price_gubun'];
		else
			target = '' + params;

		try {
			nPriceGubun = parseInt(target);
		} catch (e) {
			alert('Error : price_gubun = ' + target + ', error = ' + e.message);
		}
		if (nPriceGubun < 0 || nPriceGubun > PriceGubun.cPriceGubun.length - 1) {
			alert('Invalid price_gubun : ' + target);
			return null;
		}
		return PriceGubun.cPriceGubun[nPriceGubun];
	}

	static getAllPriceGubun() {
		return PriceGubun.cPriceGubun;
	}

	getOrderByArr() {
		return this._order_by_arr;
	}

}

function constructSel(sel, arr, sortKey = null, filterFunc = null, contentsFunc = null) {
	// remove children except first
	while(sel.children.length > 1) {
		sel.removeChild(sel.children[1]);
	}

	if (filterFunc) {
		arr = arr.slice();
		arr.forEach(function(item, index) {
			if (!filterFunc(index, item))
				delete arr[index];
		});
	}

	if (sortKey)
		arr.sort(function(a, b) { return (a[sortKey] < b[sortKey]) ? -1 : ((a[sortKey] == b[sortKey]) ? 0 : 1); });

	arr.forEach(function(r, idx) {
		if (r) { // not filtered out element
			if (contentsFunc != null)
				sel.innerHTML += contentsFunc(r);
			else {
				option = document.createElement("option");
				option.value = r['key'];
				option.innerHTML = r['name'];
				sel.appendChild(option);
			}
		}
	});

}

function initRegions(regions) {

	regionsArr = [];
	for (let i = 0; i < regions.length; i++) {
		const region = regions[i];
		gRegionsMap[region['key']] = region;
		let level = region['level'];
		if (level == 0)
			continue;
		if (level == 1)
			regionsArr.push(region);
		if (level > 1 && region['apt_yn'] != 'Y')
			continue;
		let upper = gRegionsMap[region['upper']];
		if (!upper['subregions'])
			upper['subregions'] = [];
		upper['subregions'].push(region);
	}
	let sel = document.getElementById("region_key1");
	constructSel(sel, regionsArr);
}

function clearChildSelect(sel) {
	
	constructSel(sel, []);
	if (sel.id == 'apt') {
		aptList = document.getElementById('apt');
		aptList.value = '';
		aptLabel = document.getElementById('aptLabel');
		aptLabel.textContent = '전체';
	}

	if (!sel.hasAttribute("child"))
		return;

	let child = document.getElementById(sel.getAttribute("child"));
	clearChildSelect(child);

}

function refreshRegion(sel) {

	let child = sel.getAttribute("child");
	child = document.getElementById(child);
	clearChildSelect(child);

	params = ChartManager.getChartParams();
	let value = sel.value;

	// 최상위 지역이 선택되어야 아파트별 비교 차트를 볼 수 있다

	if (sel.id.endsWith('1')) { // region_key1 
		if (value == '')
			document.getElementById('rankByAptBtn').disabled = true;
		else
			document.getElementById('rankByAptBtn').disabled = false;
	}

	// 최하위 지역을 선택하면 지역별 비교 차트를 볼 수 없다
	if (sel.id.endsWith('3')) { // region_key3
		if (value == '')
			document.getElementById('rankByRegionBtn').disabled = false;
		else
			document.getElementById('rankByRegionBtn').disabled = true;
	}
	if (value == '' || value == null) { // 전체가 선택된 거라면
		let regions = getCookie(gCookie_Region).split("|");
		if (regions.length > 1) { // 기존에 선택된 지역이 2계층 이상이라면
			// 마지막 계층의 지역을 삭제
			regions.splice(regions.length-1, 1);
			let region = regions.join("|");
			setCookie(gCookie_Region, region, 100);
		} else // 1계층이라면 삭제
			setCookie(gCookie_Region, "", 0);
		return;
	}


	let level = gRegionsMap[value]['level'];
	switch(level) {
	case 1:
		setCookie(gCookie_Region, value, 100);
		break;
	case 2:
		setCookie(gCookie_Region, document.getElementById('region_key1').value+"|"+value, 100);
		break;
	case 3:
		setCookie(gCookie_Region, document.getElementById('region_key1').value+"|"+document.getElementById('region_key2').value+"|"+value, 100);
		break;
		
	}
	if (level == 3) {
		refreshApt(child);
		return;
	}
	
	let arr = gRegionsMap[value]['subregions'];

	if (!arr)
		arr = [];

	constructSel(child, arr, 'name' /* sort by name */, function(index, item) { return item['level'] == level+1; });

}

function showMap(juso, label) {
	let container = document.getElementById('map'); //지도를 담을 영역의 DOM 레퍼런스
	let callback = function(result, status) {
    	if (status === kakao.maps.services.Status.OK) {
			container.style.display = 'block';
    		// 지도 중심을 이동 시킵니다
			let options = { //지도를 생성할 때 필요한 기본 옵션
				center: new kakao.maps.LatLng(result[0].y, result[0].x),
				level: 3 //지도의 레벨(확대, 축소 정도)
			};

			map = new kakao.maps.Map(container, options); //지도 생성 및 객체 리턴
			let marker = new kakao.maps.Marker({
        		position: new kakao.maps.LatLng(result[0].y, result[0].x),
        		text: label     
    		});
			marker.setMap(map);

			let infowindow = new kakao.maps.InfoWindow({
    			position : marker.position,
    			content : '<div style="padding:5px;"><center>' + label + '</center></div>'
			});

			infowindow.open(map, marker);
    	}
	};

	geocoder = new kakao.maps.services.Geocoder();
	geocoder.addressSearch(juso, callback);

}

function getAptTitle(params) {
	let aptName = '';
	let region_key3 = params['region_key3'];
	let region = gRegionsMap[region_key3];
	if (!region) { // called from 상위 지역의 아파트별 순위
		aptName = params['aptName'];
	} else {
	let aptArr = region['subregions'];
		for(let i = 0; i < aptArr.length; i++) {
			if (aptArr[i].key == params['apt']) {
				aptName = region['name'] + " " + aptArr[i].name;
				break;
			}
		}
	}

	return "[" + aptName + "]";
}

function refreshApt(apt) {

	clearChildSelect(apt);

	let region_key2 = document.getElementById("region_key2").value;
	let region_key3 = document.getElementById("region_key3").value;
	if (region_key2 == "" || region_key3 == "")
		return;

	let aptArr = gRegionsMap[region_key3]['subregions'];

	let func = (document.getElementById("danji").checked 
		? function(index, data) { 
			return data['danji'] == 'Y'; // data is array of id, array of name, danji_flag
		  } 
		: null); 
	let naverLinkFunc = 
		function(data) { 
			let html = "<li value="+data['key']+">";
			if (data['naver']['id'] > 0) {
				const naver_land = "https://new.land.naver.com/complexes/";
				html += data['name'];
				html += "&nbsp;&nbsp;<a href=javascript:window.open('" + naver_land + data['naver']['id'] + "');>";
				html += "<img src='static/images/naver.ico' height=16 width=16></a>";
				html += "&nbsp;<a href=\"javascript:showMap('";
				html += data['naver']['road_addr'] + "', '" + data['naver']['name'] + "');\">";
				html += "<img src='static/images/kakaomap.ico'' height=16 width=16></a>";
			} else {
				html += data['name'];
			}
			html += "</li>";
			return html;
		} 
	if (aptArr && aptArr.length > 0) { // already cached
		constructSel(document.getElementById('apt'), aptArr, -1 /* no sort */, func, naverLinkFunc);
		return;
	}

	loadingApt = true;
	url = BASE_URL + "getApt?region_key="+region_key3;
	$.getJSON(url, function(data){
		aptArr = JSON.parse(data);
		gRegionsMap[region_key3]['subregions'] = aptArr;
		constructSel(document.getElementById('apt'), aptArr, -1 /* no sort */, func, naverLinkFunc);
		const label = document.getElementById('aptLabel');
		const options = document.querySelectorAll('.aptList li');

		// 클릭한 옵션의 텍스트를 라벨 안에 넣음
		const handleSelect = (item) => {
  			label.parentNode.classList.remove('active');
			let name = item.textContent;
			name = name.endsWith("map") ? name.substring(0, name.length-3) : name; 
  			label.innerHTML = name;
			item.parentNode.value = item.value;
			item.parentNode.name = name;
			document.getElementById('apt_btn').disabled = (item.value == '');
		}

		// 옵션 클릭시 클릭한 옵션을 넘김
		options.forEach(option => {
			option.addEventListener('click', () => handleSelect(option))
		})

	}).always(function() {
		loadingApt = false;
	});
}

function getValueOfParam(params, key) {
	let input = params[key];
	if (Array.isArray(input)) {
		let value = '';
		let selected = 0;
		input.forEach(function(item) {
			if (item.checked == true) {
				value += item.value;
				selected++;
			}
		});
		if (selected == input.length) // All Selected
			value = "";
		return value;
	} else if (typeof input == "string" || typeof input == "number") {
		if (key == "region_key") {
			if (document.getElementById('region_key3').value != '') {
				params[key] = document.getElementById('region_key3').value;
				input = params[key];
			} else if (document.getElementById('region_key2').value != '') {
			    params[key] = document.getElementById('region_key2').value;
				input = params[key];
			} else if (document.getElementById('region_key1').value != '') {
			    params[key] = document.getElementById('region_key1').value;
				input = params[key];
			} else {
				input = "0000000000";
			}
		}
		return input;
	} else {
		// skip if no input, not checked
		let default_val = (input.default ? input.default : "");
		let val = input.value;
		if (!val) val = '';
		if (input.type == 'checkbox' && !input.checked)
			return default_val;
		return val;
	}
}

function checkLoadingApt(input, newValue) {
	if (loadingApt) {
		window.setTimeout(checkLoadingApt, 100, input, newValue);
	} else {
		input.value = newValue;
		let evt = document.createEvent("HTMLEvents");
		evt.initEvent("change", false, true);
		input.dispatchEvent(evt);
	}
}

function setValueOfParam(params, key, newValue) {
	let input = params[key];
	if (Array.isArray(input)) {
		let pos = 0;
		input.forEach(function(item) {
			if(item.value == newValue.substring(pos, pos+input.value_len)) {
				item.checked = true;
			}
			pos += input.value_len;
		});
	} else if (typeof input == "string" || typeof input == "number") {
		if (key == "region_key") {
			let	region = gRegionsMap[newValue];
			let level = region['level'];
			let upper = [];
			// find uper region recursivelly upward
			for(let l = level; l > 0; l--) {
				upper.push(region['key']);
				region = gRegionsMap[region['upper']];
			}
			// set region control value downward
			for(let l = 1; l <= level; l++) { 
				let region_sel = document.getElementById('region_key'+l);
				if (region_sel.value != upper[level-l]) {
					region_sel.value = upper[level-l];
					let evt = document.createEvent("HTMLEvents");
					evt.initEvent("change", false, true);
					region_sel.dispatchEvent(evt);
				}
			}
		}
		params[key] = newValue;
	} else {
		if (key == "apt") {
			let region = gRegionsMap[document.getElementById('region_key3').value];
			if (!region || !region['subregions'] || region['subregions'].length == 0) {
				window.setTimeout(checkLoadingApt, 100, input, newValue);
				return;
			}
		}
		params[key] = newValue;
		let cur_val = input.value;
		if (cur_val != newValue) {
			input.value = newValue;
			let evt = document.createEvent("HTMLEvents");
			evt.initEvent("change", false, true);
			input.dispatchEvent(evt);
		}
	}
}

function changePriceGubun(gubun) {

	
	let chartInfo = ChartManager.getCurChartInfo();
	chartInfo.priceGubun = gubun;

	let priceGubun = PriceGubun.getPriceGubun(gubun);
	let elems = document.getElementsByClassName('price_text');
	Array.from(elems).forEach(elem => elem.innerText = priceGubun.title);

	switch(ChartManager.getChartType(chartInfo.params)) {
	case CHART_TYPE.TIME_SERIES_REGION:
	case CHART_TYPE.TIME_SERIES_APT:
		ChartWrapper.setYLabel(priceGubun.title);
		redrawTimeSeriesChart();
		break;
	case CHART_TYPE.RANK_REGION:
	case CHART_TYPE.RANK_APT:
		redrawRankChart();
		break;
	case CHART_TYPE.COMPARE:
		break;
	}

}

function requestSaleStat() {

	let params = ChartManager.getChartParams();

	drawSaleStat(params);

}

function requestSaleLine() {

	let params = ChartManager.getChartParams();

	drawSaleLineChart(params);

}

function requestRankRegionChart() {

	let params = ChartManager.getChartParams();

	if (gRegionsMap[params['region_key']]['level'] == 3)
		params['region_key3'] = '';

	chartType = ChartManager.setChartType(params, CHART_TYPE.RANK_REGION);
	drawRankChart(params);
	
}

function requestRankAptChart() {

	let params = ChartManager.getChartParams();
	ChartManager.setChartType(params, CHART_TYPE.RANK_APT);

	drawRankChart(params);
}

function toggleView(divId, bRightAlign = false) {
	let div = document.getElementById(divId);
	if(div.style.display == 'block') {
		div.style.display = 'none';
	} else {
		div.style.display = 'block';
	}
	if (bRightAlign) {
		div.style.right = (document.body.clientWidth - div.offsetWidth) + 'px';
	}
}

function toggleYMTable(btn, target, desc) {
	let div;
	if (desc)
		div = document.getElementById("ym_table_desc_div");
	else
		div = document.getElementById("ym_table_asc_div");

	let elems = document.getElementsByClassName('popup');
	Array.from(elems).forEach(function(item) {
		if (item.id != div.id)
			item.style.display = 'none';
	});

	if (div.style.display == 'none') {
		div.setAttribute("target", target);
		div.style.top = (btn.offsetTop + btn.offsetHeight) + 'px';
		div.style.left = (btn.offsetLeft + btn.offsetWidth) + 'px';
		div.style.display = 'block';
		div.style.visibility = 'visible';
	} else {
		div.style.display = 'none';
		div.style.visibility = 'hidden';
	}
}

function wrapWindowByMask() { 
	//화면의 높이와 너비를 구한다. 
	let maskHeight = document.body.clientHeight; 
	let maskWidth = document.body.clientWidth; 
	
	//마스크의 높이와 너비를 화면 것으로 만들어 전체 화면을 채운다. 
	let fade = document.getElementById('fade');
	fade.style.width = maskWidth;
	fade.style.height = maskHeight; 
	fade.style.display = 'block'; 
} 
	
/// 화면의 중앙에 레이어띄움 
function showMessage(label = '조회') { 
	wrapWindowByMask(); 
	let light = document.getElementById('light');
	light.style.position = "absolute"; 
	light.style.display = 'block'; 
	light.style.top = Math.max(0, ((document.body.clientHeight - light.offsetHeight) / 2) + window.pageYOffset - 100) + "px"; 
	light.style.left = Math.max(0, ((document.body.clientWidth - light.offsetWidth) / 2) + window.pageXOffset) + "px"; 

	document.getElementById("task").innerText = label;
} 

function closeMessage() { 
	document.getElementById('fade').style.display = 'none'; 
	document.getElementById('light').style.display = 'none'; 
}

function setErrorHandler() {
	window.onerror = function (msg, url, lineNo, columnNo, error) {
		let string = msg.toLowerCase();
	    let message = [
			'Message: ' + msg,
			'Line: ' + lineNo,
			'Column: ' + columnNo,
			'Error object: ' + JSON.stringify(error)
		].join(' - ');
	
		alert(message);

		return false;
	};

	$.ajaxSetup({
		"error":function() { 
			alert("서버에서 오류가 발생했습니다.");
			let elems = document.getElementsByClassName('popup');
			Array.from(elems).forEach(elem => elem.style.display = 'none');
			closeMessage();
		}
	});
}


function makeHtml4MonthsTR(y, to_ym) {

	let html = "<tr>";
	html += "<td>" + y + "년</td>";
	for(let m = 1; m <= 12; m++) {
		let ym = (y*100 + m <= to_ym ? (y + "" + (m < 10 ? "0"+m : m)) : "");
		html += "<td class=one_ym value='" + ym + "'>" + (ym.length > 0 ? m+"월" : "") + "</td>";
	}
	html += "</tr>";

	return html;
}

// find first parent having given tag
function getParentNode(elem, tag) {

	let parent = elem.parentElement;
	if (parent == null)
		return null;
	if (parent.tagName == tag)
		return parent
	
	return getParentNode(parent, tag);
}

function initSelectYMTable() {

	const MIN_YY = 2006;
	const DEFAULT_PERIOD = 3;

	let today = new Date();
	let yy = today.getFullYear();
	let mm = today.getMonth()+1;
	let to_ym = yy * 100 + mm;
	// from_ym set to 3 years minus one month ago
	let from_ym = (mm == 12 ? (yy - (DEFAULT_PERIOD - 1)) * 100 + 1 : (yy - DEFAULT_PERIOD) * 100 + mm + 1);

	for(let y = MIN_YY; y < to_ym / 100; y++) {
		let html = makeHtml4MonthsTR(y, to_ym);
		document.querySelector('#ym_table_asc > tbody').innerHTML += html;
	};
	document.querySelector('input#from_ym').value = from_ym;
	document.querySelector('input#to_ym').value = to_ym;
	document.querySelector('input#base_ym').value = to_ym;
	for(let y = Number.parseInt(to_ym / 100); y >= MIN_YY; y--) {
		let html = makeHtml4MonthsTR(y, to_ym);
		document.querySelector('#ym_table_desc > tbody').innerHTML += html;
	};

	let elems = document.getElementsByClassName('one_ym');
	Array.from(elems).forEach(function(elem) {
		elem.addEventListener("click", function() {
			let div = getParentNode(this, 'DIV');
			let target = div.getAttribute("target");
			let ym = this.getAttribute("value");
			if (ym && ym != '') {
				document.getElementById(target).value = ym;
				div.style.display = 'none';
			}
		});
		elem.addEventListener("mouseover", function(){
			let ym = this.getAttribute("value");
			if (ym && ym != '') {
				this.style.backgroundColor = "yellow";
			}
		});
		elem.addEventListener("mouseout", function(){
			let ym = this.getAttribute("value");
			if (ym && ym != '') {
				this.style.backgroundColor = this.parentNode.style.backgroundColor;
			}
		});
	});

}

function setPopupPosition(event, popup) { 

	let mousePosition = {}; 
	let popupPosition = {}; 
	let menuDimension = {}; 

	menuDimension.x = popup.offsetWidth; 
	menuDimension.y = popup.offsetHeight; 
	mousePosition.x = event.pageX; 
	mousePosition.y = event.pageY; 

	//if (mousePosition.x + menuDimension.x > document.body.clientWidth + window.pageXOffset) { 
	if (mousePosition.x > document.body.clientWidth / 2) { 
		popupPosition.x = Math.max(mousePosition.x - menuDimension.x, 10);
	} else { 
		popupPosition.x = mousePosition.x; 
	} 
	popupPosition.x = Math.max(0, Math.min(popupPosition.x, document.body.clientWidth - menuDimension.x));

	popupPosition.y = Math.max(0, Math.min(document.body.clientHeight - menuDimension.y, mousePosition.y));
	/*
	if (mousePosition.y + menuDimension.y > document.body.clientHeight + window.pageYOffset) { 
		popupPosition.y = Math.max(mousePosition.y - menuDimension.y, 10); 
	} else { 
		popupPosition.y = mousePosition.y; 
	}*/

	return popupPosition; 
} 

let colorArr = ['#000000', '#00CC00', '#00FFFF','#FFFF00','#0066FF', '#CC0000', '#660099', '#66FF00', '#CC9999', '#CC66FF'];
let curColor = 0;

function getNextChartColor() {
	
    curColor++;
	if (curColor > colorArr.length)
		curColor = 1;
														      
	return colorArr[curColor-1];
}  

function resetChartColor() {
	curColor = 0;
}

function showAptSaleTable(event, data) {

	document.querySelector('#aptSaleList > tbody').innerHTML = '';
	data.forEach(function(r, idx) {
		let area = parseFloat(r[1]);
		let price = parseInt(r[3].replace(/,/g,""));
		let unit_price = (price / (area/3.3)).toFixed(1);
		unit_price = unit_price.toString().replace(/(\d)(?=(?:\d{3})+(?!\d))/g, "$1,");
		let html = '<tr class=aptSaleListLine>';
		html += "<td class=aptSaleListItem style='text-align: center;'>" + r[0] + '</td>';
		html += "<td class=aptSaleListItem style='text-align: right;'>" + r[1] + ' (' + (area/3.3).toFixed(1) + '평)</td>';
		html += "<td class=aptSaleListItem style='text-align: center;'>" + r[2] + '</td>';
		html += "<td class=aptSaleListItem style='text-align: right;'>" + r[3] + '</td>';
		html += "<td class=aptSaleListItem style='text-align: right;'>" + unit_price + '</td>';
		html += "</tr>";
		document.querySelector('#aptSaleList > tbody').innerHTML += html;
	});
	let div = document.getElementById("aptSaleDiv");
	div.style.display = 'block';
	div.style.visibility = 'visible';
	let pos = setPopupPosition(event, div);
	div.style.top = pos.y+'px';
	div.style.left = pos.x+'px';
}

function hideChart(btn, index) {

	let legend = document.getElementById('legend'+index);
	if (legend.style.textDecoration.startsWith('line-through')) {
		ChartWrapper.showChart(index, true);
		legend.style.textDecoration = 'none';
		btn.title = '숨기기';
		btn.style.backgroundImage = "url(static/images/hide.png)"; 	
	} else {
		ChartWrapper.showChart(index, false);
		legend.style.textDecoration = 'line-through';
		btn.title = '보이기';
		btn.style.backgroundImage = "url(static/images/show.png)"; 	
	}

}

function makeRankChart2TableHTML(data) {

	let html = "<tr><th>구분</th>";
	//html += "<th>"+ChartWrapper.getChartLabel(0)+"</th>";
	html += "<th>변동율(%)</th>";
	html += "<th>"+ChartWrapper.getChartLabel(1)+"</th>";
	html += "<th>"+ChartWrapper.getChartLabel(2)+"</th>";
	html += "</tr>";
	for(let i = 0; i < data['labels'].length; i++) {
		html += "<tr>";
		html += "<td>"+data['labels'][i]+"</td>";
		html += "<td>"+ChartWrapper.getChartData(0)[i]+"</td>";
		html += "<td>"+ChartWrapper.getChartData(1)[i]+"</td>";
		html += "<td>"+ChartWrapper.getChartData(2)[i]+"</td>";
		html += "</tr>";
	}
	return html;
}

function excelTimeSeriesChart(index) {

	let title = ChartWrapper.getChartLabel(index);

	let chartInfo = ChartManager.getChartInfo(title);
	const priceGubun = chartInfo.priceGubun;

	let html = "<tr>";
	html += "<th>년월</th>";
	html += "<th>평균 " + priceGubun.title + "</th>";
	if (chartInfo.data['ma']) {
		html += "<th>평균 " + priceGubun.title + "(1YR)</th>";
	}
	if (chartInfo.data['cnt']) {
		html += "<th>거래건수</th>";
	}
	html += "</tr>";
	let labels = Object.values(ChartWrapper.getChartLabels());
	let data = Object.values(ChartWrapper.getChartData(index));
	let volume_data = null;
	let ma_data = null;
	if (chartInfo.data['cnt']) {
		volume_data = Object.values(chartInfo.data['cnt']);
	}
	if (chartInfo.data['ma']) {
		ma_data = Object.values(chartInfo.data[priceGubun.ma_name]);
	}
	for(let i = 0; i < data.length; i++) {
		html += "<tr>";
		html += "<td>"+labels[i]+"</td>";
		html += "<td>"+(data[i]==null?"":data[i])+"</td>";
		if (chartInfo.data['ma']) {
			html += "<td>"+(ma_data[i]==null?"":ma_data[i])+"</td>";
		}
		if (chartInfo.data['cnt']) {
			html += "<td>"+(volume_data[i]==null?"":volume_data[i])+"</td>";
		}
		html += "</tr>";
	}
	table2excel(title, html);
}

function excelRankChart() {

	let title = ChartWrapper.getChartLabel(0);

	let chartInfo = ChartManager.getChartInfo(title);

	url = ChartManager.getChartType(chartInfo.params).url(chartInfo.params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		table2excel(title, makeRankChart2TableHTML(data) );
		closeMessage();
	});
}

function table2excel(title, table) {
	let tab_text = '<html xmlns:x="urn:schemas-microsoft-com:office:excel">';
	tab_text += '<head><meta http-equiv="content-type" content="application/vnd.ms-excel; charset=UTF-8">';
	tab_text += '<xml><x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet>'
	tab_text += '<x:Name>Chart Data</x:Name>';
	tab_text += '<x:WorksheetOptions><x:Panes></x:Panes></x:WorksheetOptions></x:ExcelWorksheet>';
	tab_text += '</x:ExcelWorksheets></x:ExcelWorkbook></xml></head><body>';
	tab_text += "<table border='1px'>";

	tab_text += table;

	tab_text += '</table></body></html>';
	let data_type = 'data:application/vnd.ms-excel';
	let ua = window.navigator.userAgent;
	let msie = ua.indexOf("MSIE ");
	let fileName = title + '.xls';
	//Explorer 환경에서 다운로드
	if (msie > 0 || !!navigator.userAgent.match(/Trident.*rv\:11\./)) {
		if (window.navigator.msSaveBlob) {
			let blob = new Blob([tab_text], {
				type: "application/csv;charset=utf-8;"
			});
			navigator.msSaveBlob(blob, fileName);
		}
	} else {
		let blob2 = new Blob([tab_text], {
			type: "application/csv;charset=utf-8;"
		});
		let filename = fileName;
		let elem = window.document.createElement('a');
		elem.href = window.URL.createObjectURL(blob2);
		elem.download = filename;
		document.body.appendChild(elem);
		elem.click();
		document.body.removeChild(elem);
	}
}

function excelSales(index, apt) {

	let key = ChartWrapper.getChartLabel(index);
	let chartInfo = ChartManager.getChartInfo(key);
	let aptName = getAptTitle(chartInfo.params);

	const url = getChartURL('getAptSale', { 'apt': apt });
	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		let html = "<tr>";
		html += "<th>거래일</th>";
		html += "<th>전용면적(제곱미터)</th>";
		html += "<th>전용면적(평)</th>";
		html += "<th>층</th>";
		html += "<th>거래금액(만원)</th>";
		html += "<th>평단가(만원)</th>";
		html += "</tr>";
		data.forEach(function(r, idx) {
	  		let area = parseFloat(r[1]);
	  		let price = parseInt(r[3].replace(/,/g,""));
			html += "<tr>";
			html += "<td>" + r[0] + "</td>";
			html += "<td>" + r[1] + "</td>";
			html += "<td>" + (area/3.3).toFixed(1) + "</td>";
			html += "<td>" + r[2] + "</td>";
			html += "<td>" + r[3] + "</td>";
			let unit_price = (price / (area/3.3)).toFixed(1);
			unit_price = unit_price.toString().replace(/(\d)(?=(?:\d{3})+(?!\d))/g, "$1,");
			html += "<td>" + unit_price + "</td>";
			html += "</tr>";
		});
		table2excel(aptName+'(거래목록)', html);
		closeMessage();
	});
}

function delChart(index) {

	let label = ChartWrapper.getChartLabel(index);
	ChartManager.removeChart(label);


}

function setLineThick(index, thick, recursive) {

	ChartWrapper.setChartThick(index, thick);
	if (recursive) return;

	let label = ChartWrapper.getChartLabel(index);
	// 기본 차트에 이동평균 또는 거래량 차트가 추가되어 있으면
	for(i = index+1; i < ChartWrapper.getChartCount(); i++) {
		if (ChartWrapper.getChartLabel(i).startsWith(label)) {
			setLineThick(i, thick, true);
		}
	}

}

function getChartURL(urlName, params) {

	let url = BASE_URL + urlName + "?"

	let p = params;
	if (params.params) p = params.params;

	for (const key in p) {
		let value = params[key];
		if (value == null) value = '';
		url += key + "=" + value + "&";
	}

	return url;

}

function getConditionParam(params, param, defaultValue = null) {
	
	let value = params[param];
	if (value == null || value == '') {
		if (defaultValue != null) {
			value = defaultValue;
			params[param] = value;
		} else {
			value = '';
		}
	}

	return value;

}

function drawTimeSeriesLegend(chart) {

	let text = []; 

	for (i = 0, j = 0; i <chart.data.datasets.length; i++) { 
		let chartInfo = ChartManager.getChartInfo(chart.data.datasets[i].label);
		if (!chartInfo)
			continue;
		if (j > 0)
			text.push("&nbsp;|&nbsp;");
		let color = chart.data.datasets[i].backgroundColor ? chart.data.datasets[i].backgroundColor : chart.options.defaultColor;
		text.push('<span style="background-color: ' + color + ';">&nbsp;&nbsp;&nbsp;&nbsp;</span>'); 
		text.push('<span class="line" id="legend'+i+'" style="font-size:small;" onmouseover="setLineThick('+i+', true);" onmouseout="setLineThick('+i+', false);" >'); 
		text.push(chart.data.datasets[i].label+'</span>');
		text.push("<input type=button title='숨기기' class='chart-btn chart-btn-hide' onclick='hideChart(this, "+i+");' />");
		text.push("<input type=button title='삭제' class='chart-btn chart-btn-del' onclick='delChart("+i+");' />");
		text.push("<input type=button title='엑셀' class='chart-btn chart-btn-excel' onclick='excelTimeSeriesChart("+i+");' />");
		const apt = chartInfo.apt;
		const region = chartInfo.region_key;
		if (apt != undefined && apt != "" && gRegionsMap[region]['level'] == 3) {
			text.push("<input type=button title='거래내역엑셀' class='chart-btn chart-btn-excel' onclick='excelSales("+i+","+apt+");' />");
		}
		j++;
	}

	return text.join("");

}

function getChartDataAtEvent(clickedDatasetIndex) {

	let key = null;
	let chartType = ChartManager.getCurChartType();
	switch(chartType) {
	case CHART_TYPE.TIME_SERIES_REGION:
	case CHART_TYPE.TIME_SERIES_APT:
		key = ChartWrapper.getChartLabel(clickedDatasetIndex);
		break;
	case CHART_TYPE.RANK_REGION:
	case CHART_TYPE.RANK_APT:
		key = ChartWrapper.getChartLabel(0); // key is always 1st chart
		break;
	case CHART_TYPE.COMPARE:
		return null;
	}

	return ChartManager.getChartInfo(key, clickedDatasetIndex);
}

function wait4LoadingApt(params) {
	if (loadingApt) {
		window.setTimeout(wait4LoadingApt, 100, params);
	} else {
		window.setTimeout(drawSaleLineChart, 100, params);
	}
}

function goForSaleLineChart(chartInfo, label, dataIndex) {

	let base_ym = chartInfo.base_ym;
	let years = chartInfo.years;
	let yy = parseInt(base_ym.substr(0,4)) - parseInt(years) - 1;
	let mm = parseInt(base_ym.substr(4,2));
	if (mm == 12) {
		yy++;
		mm = 1;
	} else
		mm++;
	let from_ym = '' + yy + (mm < 10 ? "0"+mm : mm);

	let params = deep_copy_params(chartInfo.params);

	let newChartInfo = new ChartInfo(params, null);
	newChartInfo.from_ym = from_ym;
	newChartInfo.to_ym = chartInfo.base_ym;

	if (chartInfo.data['apt'][dataIndex] == '') {
		newChartInfo.region_key = chartInfo.data['region_key'][dataIndex];
		drawSaleStat(newChartInfo.params);
	} else {
		newChartInfo.aptName = label;
		newChartInfo.apt = chartInfo.data['apt'][dataIndex];
		window.setTimeout(wait4LoadingApt, 100, newChartInfo.params);
	}
}

function goForRankChartOrAptSaleTable(chartInfo, label, evt) {
	if (ChartManager.getCurChartType() == CHART_TYPE.TIME_SERIES_APT) {
		chartInfo.base_ym = label;
		$.getJSON(getChartURL('getAptSale',chartInfo.params), function(jsonData){
			data = JSON.parse(jsonData);
			showAptSaleTable(evt, data);
		});
	} else {
		let params = deep_copy_params(chartInfo.params);
		let newChartInfo = new ChartInfo(params, null);
		const region = gRegionsMap[chartInfo.region_key];
		if (region['level'] == 3)
			ChartManager.setChartType(params, CHART_TYPE.RANK_APT);
		else
			ChartManager.setChartType(params, CHART_TYPE.RANK_REGION);
		newChartInfo.base_ym = label;
		newChartInfo.years = 1;
		drawRankChart(newChartInfo.params);
	}
}

function chartClickEventHandler(evt) {
	let indices = ChartWrapper.getDataIndicesOfEvent(evt);

	if (!indices || !indices.length)
		return;

	let clickedDatasetIndex = indices[0];
	let clickedDataIndex = indices[1];

	let chartInfo = getChartDataAtEvent(clickedDatasetIndex);
	const errorMsg = "Can't find chartData";
	console.assert(chartInfo, {clickedDatasetIndex: clickedDatasetIndex, errorMsg: errorMsg});

	let labels = ChartWrapper.getChartLabels();
	let label = labels[clickedDataIndex];

	switch (ChartManager.getChartType(chartInfo.params)) {
		case CHART_TYPE.TIME_SERIES_REGION:
		case CHART_TYPE.TIME_SERIES_APT:
			goForRankChartOrAptSaleTable(chartInfo, label, evt);
			break;
		case CHART_TYPE.COMPARE:
			break;
		case CHART_TYPE.RANK_APT:
			goForSaleLineChart(chartInfo, label, clickedDataIndex);
			break;
		case CHART_TYPE.RANK_REGION:
			// drill down to sub-region
			goForSaleLineChart(chartInfo, label, clickedDataIndex);
			break;
	}

	document.getElementById("aptSaleDiv").style.display = 'none';
}


function getMonths(ym1, ym2) {
	let min_ym, max_ym;
	if (ym1 < ym2) {
		min_ym = ym1;
		max_ym = ym2;
	} else {
		min_ym = ym2;
		max_ym = ym1;
	}
	let years = parseInt(max_ym.substr(0, 4)) - parseInt(min_ym.substr(0, 4));
	let months = parseInt(max_ym.substr(4, 2)) - parseInt(min_ym.substr(4, 2));
	return years * 12 + months;

}

function lpadData(ym1, ym2, data, volume_data) {

	let months = getMonths(ym1, ym2);
	for(let i = 0; i < months; i++) {
		data.unshift(null);
		if (volume_data)
			volume_data.unshift(null);
	}

}

function rpadData(ym1, ym2, data, volume_data) {

	let months = getMonths(ym1, ym2);
	for(let i = 0; i < months; i++) {
		data.push(null);
		if (volume_data)
			volume_data.unshift(null);
	}
}

function lpadLabels(minYm, labels) {
	while (labels[0] > minYm) {
		let year = parseInt(labels[0].substr(0, 4));
		let mm = parseInt(labels[0].substr(4, 2));
		mm = mm - 1;
		if (mm == 0) {
			year = year - 1;
			mm = 12;
		}
		labels.unshift(year.toString() + (mm < 10 ? "0"+mm.toString() : mm.toString()));
	}
}

function rpadLabels(maxYm, labels) {
	while (labels[labels.length-1] < maxYm) {
		let year = parseInt(labels[labels.length-1].substr(0, 4));
		let mm = parseInt(labels[labels.length-1].substr(4, 2));
		mm = mm + 1;
		if (mm > 12) {
			year = year + 1;
			mm = 1;
		}
		labels.push(year.toString() + (mm < 10 ? "0"+mm.toString() : mm.toString()));
	}
}

function spreadLabels(labels, data, volume_data) {
	let chartLabels = ChartWrapper.getChartLabels();
	if (chartLabels[0] < labels[0]) {
		lpadData(chartLabels[0], labels[0], data, volume_data);
	} else if (chartLabels[0] > labels[0]) {
		for(let i = 0; i < ChartWrapper.getChartCount(); i++) {
			lpadData(chartLabels[0], labels[0], ChartWrapper.getChartData(i));
		}
		lpadLabels(labels[0], chartLabels);
	}
	if (chartLabels[chartLabels.length-1] > labels[labels.length-1]) {
		rpadData(chartLabels[chartLabels.length-1], labels[labels.length-1], data, volume_data);
	} else if (chartLabels[chartLabels.length-1] < labels[labels.length-1]) {
		for(let i = 0; i < ChartWrapper.getChartCount(); i++) {
			rpadData(chartLabels[chartLabels.length-1], labels[labels.length-1], ChartWrapper.getChartData(i));
		}
		rpadLabels(labels[labels.length-1], chartLabels);
	}
}


function makeRelativeData(base_pos, data) {

	let new_data = [];
	// 그 데이터를 기준(100)으로 다 상대 수치로 변환한다
	let base = data[base_pos];
	for (let i = 0; i < data.length; i++) {
		if (data[i] != null) 
			new_data[i] = ((data[i] / base) * 100).toFixed(1);
	}

	return new_data;
}


function setDataRelative(data) {
	// null이 아닌 첫번째 데이터를 찾아서
	let base_pos = 0;
	while (true) {
		if (data[base_pos] == null && base_pos < data.length)
			base_pos++;
		else
			break;
	}
	if (base_pos == data.length) 
		base_pos--;


	return base_pos;
}

/*
	chartData = { 'label', 'type', 'data', yAxe { id, direction, label }, color, chartOptions }
*/
function drawTimeSeriesChartInner(title, data, priceGubun, chartOptions = {}){

	let base_pos = -1;
	for (let i = 0; i < data.length; i++) {
		let chartOptions = data[i].chartOptions;
		if (!chartOptions)
			chartOptions = {};

		if (!chartOptions['backgroundColor']) {
			let bgColor = Chart.defaults.global.defaultColor;
			if (ChartManager.isDrawOverlap())
				bgColor = getNextChartColor();
			chartOptions['backgroundColor'] = bgColor;
		}

		if (data[i].yAxe.position == YAXE_POSITION.RIGHT)
			ChartWrapper.addRightYAxe(data[i].yAxe.label, data[i].yAxe.id);

		ChartWrapper.makeChartDataset(data[i].type, title, data[i].yAxe.id, data[i].data, data[i].color, chartOptions);

	}

}

function redrawTimeSeriesChart() {
	
	ChartWrapper.emptyChart();

	// 보관하고 있는 원래의 데이터로 다시 차트를 그린다
	let sel = document.getElementById("chartHist");
	let key_arr = sel.options[sel.selectedIndex].value.split(CHART_KEY_SEPARATOR);
	for(let key of key_arr) {
		let chartInfo = ChartManager.getChartInfo(key);
		drawTimeSeriesChartBase(key, chartInfo.data, chartInfo.params);
	}
}

function drawTimeSeriesChartBase(title, data, params){

	let map = document.getElementById("map");
	if (map)
		map.style.display = 'none';

	const priceGubun = PriceGubun.getPriceGubun(params);

	let chartData = [];
	let subChart = {};
	subChart['data'] = data[priceGubun.price_name];

	let base_pos = -1;
	// 상대수치로 그리기 라면
	if (ChartManager.isDrawRelative()) { 
		base_pos = setDataRelative(subChart['data']);
		subChart['data'] = makeRelativeData(base_pos, subChart['data']);
	}
	subChart['type'] = 'line';
	subChart['label'] = title;
	subChart['color'] = 'red';
	subChart['yAxe'] = { 'position': YAXE_POSITION.LEFT, 'id': 'A' };
	chartData.push(subChart);

	// 거래량 같이 그리기
	if (ChartManager.isDrawVolume() && data['cnt']) {
		subChart = {};
		subChart['data'] = data['cnt'];
		subChart['type'] = 'bar';
		subChart['label'] = title + "(거래건수)";
		subChart['color'] = 'yellow';
		subChart['yAxe'] = { 'position': YAXE_POSITION.RIGHT, 'id': 'B', 'label': '거래건수' };
		chartData.push(subChart);
	}

	// 6개월 이동평균으로 그리기
	if (ChartManager.isDrawMA() && data[priceGubun.ma_name]) {
		subChart = {};
		subChart['data'] = data[priceGubun.ma_name];
		if (ChartManager.isDrawRelative()) 
			subChart['data'] = makeRelativeData(base_pos, subChart['data']);
		subChart['type'] = 'line';
		subChart['label'] = title + "(1YR)";
		subChart['color'] = 'blue';
		subChart['yAxe'] = { 'position': YAXE_POSITION.LEFT, 'id': 'A' };
		chartData.push(subChart);
	}

	drawTimeSeriesChartInner (title, chartData, priceGubun);

}

function drawNewChartCommon(title, data, params, yLabel, tooltipFunc = null) {

	// 팡업 화면을 숨긴다
	document.getElementById("aptSaleDiv").style.display = 'none';

	ChartManager.createChart(title, data, params, yLabel, tooltipFunc);

}

function drawTimeSeriesChart(title, data, params){

	const priceGubun = PriceGubun.getPriceGubun(params);
	const mainData = data[priceGubun.price_name];
	const volumeData = data['cnt'];
	const yLabel = priceGubun.title;

	drawNewChartCommon(title, data, params, yLabel);

	// 기존에 그려진 차트가 있으면
	if (ChartWrapper.getChartCount() > 0) {
		// 새 데이터와 기존 데이터의 X 라벨의 From~To를 맞춰준다
		spreadLabels(data['labels'], mainData, volumeData);
	}
	
	// 차트용 데이터를 구성하고 차트를 그린다
	drawTimeSeriesChartBase(title, data, params, true);

	ChartWrapper.chartComplete(drawTimeSeriesLegend);
}

function drawSaleLineChart(params) {

	const apt = params['apt']
	if (apt == undefined || apt == "") {
		alert('아파트가 선택되지 않았습니다.');
		return;
	}

	let chartType = ChartManager.setChartType(params, CHART_TYPE.TIME_SERIES_APT);
	let title = chartType.title(params);

	if (ChartManager.isDrawOverlap() && ChartManager.existsInCurChart(title)) {
		alert("이미 있는 차트입니다.");
		return;
	}

	let url = chartType.url(params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		drawTimeSeriesChart(title, data, params);
		closeMessage();
	});

}

function makeChartConditionTitle(params, need_params) {

	// check if any param exists
	let existsParams = false;
	let map = need_params.reduce((obj, arr) => {
		obj[arr] = true;
		return obj;
	}, {});

	let tmp_params = params;
	//let tmp_params = params.params ? params.params : params;
	for(const key in tmp_params) {
		if (key == 'region_key')
			continue;
		if (tmp_params[key] != '' && map[key]) {
			existsParams = true;
			break;
		}
	}

	// if no param, just return title
  	if (!existsParams)
  		return "";

	let needsComma = false;
	let title = " (";
	if (map['danji']) {
		const danji = getConditionParam(tmp_params, 'danji');
		if (danji == "Y"){
   			title += "300세대 이상만";
		} else {
    		title += "모든 실거래";
		}
		needsComma = true;
	}
	if (map['from_ym'] && map['to_ym']) {
		const from_ym = getConditionParam(tmp_params, 'from_ym');
		const to_ym = getConditionParam(tmp_params, 'to_ym');
    	if (from_ym + to_ym != ""){
			if (needsComma) title += ", ";
			else needsComma = true;
      		title += "기간 : " + from_ym + " ~ " + to_ym;
    	}
	}
	if (map['ages'] && map['age_sign']) {
		const ages = getConditionParam(tmp_params, 'ages');
		const age_sign = getConditionParam(tmp_params, 'age_sign');
    	if (ages != '' && age_sign != ''){
			if (needsComma) title += ", ";
			else needsComma = true;
      		title += "연식 : " + ages + " 년 " + (age_sign == '<' ? "이내" : "이상");
    	}
	}
	if (map['area_type']) {
		const area_type = getConditionParam(tmp_params, 'area_type');
    	if (area_type != ""){
			if (needsComma) title += ", ";
			else needsComma = true;
      		title += "전용면적 : ";
      		pos = 0;
      		do {
        		if (pos > 0) title += "|";
        		switch (area_type.substr(pos, 2)) {
          		case "01":
            		title += "~60";
            		break;
          		case "02":
            		title += "60~85";
            		break;
          		case "03":
            		title += "85~135";
            		break;
          		case "04":
            		title += "135~";
            		break;
        		}
        		pos += 2;
      		} while (area_type.length > pos);
		}
    }
	if (map['base_ym'] && map['years']) {
		const base_ym = getConditionParam(tmp_params, 'base_ym');
		const years = getConditionParam(tmp_params, 'years');
    	if (base_ym != "" && years != ""){
			if (needsComma) title += ", ";
			else needsComma = true;
			title += "기준년월 : " + base_ym + ", 비교대상 : " + years + "년 전"; 
		}
	}
    title += ")";
 
 	return title;

}

function drawSaleStat(params) {
	
	ChartManager.setChartType(params, CHART_TYPE.TIME_SERIES_REGION);

	let chartType = ChartManager.getChartType(params);
	let title = chartType.title(params);

	if (ChartManager.isDrawOverlap() && ChartManager.existsInCurChart(title)) {
		alert("이미 있는 차트입니다.");
		return;
	}

	let url = chartType.url(params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		drawTimeSeriesChart(title, data, params);
		closeMessage();
	});
}

function drawRankLegend(chart) {

	let text = [];

	let chartInfo = ChartManager.getChartInfo(ChartWrapper.getChartLabel(0));
	let chartParams = chartInfo.params;

	text.push('<center>');
	if (chartParams.hasPrior)
		text.push("<span><a onclick='drawRankChartPage(false);'>◀&nbsp;&nbsp;</a></span>");

	for (i = 0; i <chart.data.datasets.length; i++) { 
		if (i > 0)
			text.push("&nbsp;|&nbsp;");
		let color = ChartWrapper.getChartBgcolor(i);
		text.push('<span style="background-color: ' + color + ';">&nbsp;&nbsp;&nbsp;&nbsp;</span>'); 
		text.push('<span style="font-size:small;">' +ChartWrapper.getChartLabel(i) + '</span>');
	}
	text.push("&nbsp;|&nbsp;");
	let curOrderby = chartParams['orderby'];
	text.push("<select id=rank_orderby class='chart-btn chart-btn-excel' onchange='redrawRankChart();'>");
	const priceGubun = PriceGubun.getPriceGubun(chartParams);
	const orderByOptions = priceGubun.getOrderByArr();
	orderByOptions.forEach(function(orderbyOption) {
		text.push("<option value="+orderbyOption.var_name+(curOrderby == orderbyOption.var_name ? " selected":"") + ">"+orderbyOption.title+"</option>");
	});
	text.push("</select>");
	text.push("<button class='chart-btn chart-btn-excel' onclick='excelRankChart();'>엑셀 다운로드</button>");
	if (chartParams.hasLater)
		text.push("<span><a onclick='drawRankChartPage(true);'>&nbsp;&nbsp;▶</a></span>");

	text.push('</center>');
	return text.join("");
}

function redrawRankChart() {
	
	let chartInfo = ChartManager.getChartInfo(ChartWrapper.getChartLabel(0));

	let selectedIndex = document.getElementById('rank_orderby').selectedIndex;
	const priceGubun = chartInfo.priceGubun;
	const orderByOptions = priceGubun.getOrderByArr();
	chartInfo.order_by = orderByOptions[selectedIndex].var_name;

	chartInfo.page = 1;

	drawRankChart(chartInfo.params);

}

function drawRankChartPage(next) {

	
	let chartInfo = ChartManager.getChartInfo(ChartWrapper.getChartLabel(0));
	let page = chartInfo.page;
	if (next)
		page += 1;
	else
		page -= 1;
	chartInfo.page = page;

	drawRankChart(chartInfo.params);
		
}

function getCurYM() {
	let today = new Date();   
		
	let year = today.getFullYear(); // 년도
	let month = today.getMonth() + 1;
	ym = year + "" + (month<10?"0"+month:month);

	return ym;
}

function getRegionTitle(params) {
	let title = '';

	region = gRegionsMap[params['region_key']];

	switch( region['level']) {
	case 0:
		title = '전체';
		break;
	case 1:
		title = region['name'];
		break;
	case 2:
		upper_region = gRegionsMap[region['upper']];
		title = upper_region['name'];
		title += " " + region['name'];
		break;
	case 3:
		upper_region = gRegionsMap[region['upper']];
		upper_upper_region = gRegionsMap[upper_region['upper']];
		title = upper_upper_region['name'];
		title += " " + upper_region['name'];
		title += " " + region['name'];
		break;
	}
		
	return '[' + title + ']';
}

function getTooltip4RankChart(tooltipItem, data) {
	if (tooltipItem.length != 1) return;
	if (tooltipItem[0].datasetIndex != 1 && tooltipItem[0].datasetIndex != 2) return;
	let label1 = ChartWrapper.getChartLabel(tooltipItem[0].datasetIndex);
	let label2 = ChartWrapper.getChartLabel(tooltipItem[0].datasetIndex % 2 + 1);
	let chartData = ChartWrapper.getChartData(tooltipItem[0].datasetIndex % 2 + 1);
	let body = label1 + ": " + tooltipItem[0].value + " / " + label2 + ": " + chartData[tooltipItem[0].index];
	tooltipItem.pop();
	return body;
}

function setPageNavigator(params, data) {
	let page = params['page'];

	if (page > 1)
		params['hasPrior'] = true;
	else
		params['hasPrior'] = false;

	if (data['has_more'])
		params['hasLater'] = true;
	else
		params['hasLater'] = false;
}

function drawRankChartBase(title, data, params) {

	drawNewChartCommon(title, data, params, '변동율(%)', getTooltip4RankChart);

	setPageNavigator(params, data);

	const priceGubun = PriceGubun.getPriceGubun(params);
	let price_name = priceGubun.price_name;
	let rate_name = priceGubun.rate_name;
	let yLabel = priceGubun.title;
	let bprice_name = priceGubun.before_price_name;

	ChartWrapper.setXAxeForDualBar();
	ChartWrapper.addRightYAxe(yLabel, 'B');

	ChartWrapper.makeChartDataset('line', title, 'A', data[rate_name]);
	ChartWrapper.makeChartDataset('bar', params['base_ym'] + '의 ' + yLabel, 'B', data[price_name], null,
		{
			backgroundColor : 'rgba(0, 255, 0, 0.5)',
			barPercentage: 0.9 
		});
	ChartWrapper.makeChartDataset('bar', params['years'] + '년 전의' + yLabel, 'B', data[bprice_name], null, 
		{
			backgroundColor : 'rgba(255, 0, 0, 1)', 
			xAxisID: 'X2', 
			barPercentage: 0.5 
		});

	ChartWrapper.chartComplete(drawRankLegend);

}

function drawRankChart(params) {
	
	let map = document.getElementById("map");
	if (map)
		map.style.display = 'none';

	const region = gRegionsMap[params['region_key']];

	if (!params['orderby'] || params['orderby'] == "")
		params['orderby'] = PriceGubun.getPriceGubun(params).getOrderByArr()[0].var_name;
	if (!params['page'] || params['page'] < 1)
		params['page'] = 1;
	
	let chartType = ChartManager.getChartType(params);

	let url = chartType.url(params);
	let title = chartType.title(params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);

		drawRankChartBase(title, data, params);

		closeMessage();
	});
}

function initCustomSelect() {

	const label = document.getElementById('aptLabel');

	// 라벨을 클릭시 옵션 목록이 열림/닫힘
	label.addEventListener('click', () => {
		if (event.buttons != 0)
			return true;
  		if(label.parentNode.classList.contains('active')) {
  			label.parentNode.classList.remove('active');
	  	} else {
  			label.parentNode.classList.add('active');
  		}
	});
	label.addEventListener('blur', () => {
  		label.parentNode.classList.remove('active');
	});

}

function getCookie(name) { //가져올 쿠키의 이름을 파라미터 값으로 받고

	let nameOfCookie = name + "="; //쿠키는 "쿠키=값" 형태로 가지고 있어서 뒤에 있는 값을 가져오기 위해 = 포함

	let x = 0;

	while (x <= document.cookie.length) {  //현재 세션에 가지고 있는 쿠키의 총 길이를 가지고 반복

		let y = (x + nameOfCookie.length); //substring으로 찾아낼 쿠키의 이름 길이 저장

		if (document.cookie.substring(x, y) == nameOfCookie) { //잘라낸 쿠키와 쿠키의 이름이 같다면

			if ((endOfCookie = document.cookie.indexOf(";", y)) == -1) //y의 위치로부터 ;값까지 값이 있으면 

				endOfCookie = document.cookie.length; //쿠키의 길이로 적용하고

			return unescape(document.cookie.substring(y, endOfCookie)); //쿠키의 시작점과 끝점을 찾아서 값을 반환

		}

		x = document.cookie.indexOf(" ", x) + 1; //다음 쿠키를 찾기 위해 시작점을 반환
		if (x == 0) //쿠키 마지막이면 
			break; //반복문 빠져나오기

	}

	return ""; //빈값 반환

}

function setCookie(cName, cValue, cDay){
	let expire = new Date();
	expire.setDate(expire.getDate() + cDay);
	cookies = cName + '=' + escape(cValue) + '; path=/ '; // 한글 깨짐을 막기위해 escape(cValue)를 합니다.
	if (typeof cDay != 'undefined') 
		cookies += ';expires=' + expire.toGMTString() + ';';
	document.cookie = cookies;
}

function makeScatterDataset(data, label, prefix, priceGubun, bgColor, rotation = 0) {

	let scatter_data = []
	for (let i = 0; i < data['labels'].length; i++) {
		scatter_data.push({ x: i*4+1, y: data[prefix + priceGubun.price_name][i] });
	}

	ChartWrapper.makeChartDataset('scatter', label, 'B', scatter_data, 'red', 
		{ 
			backgroundColor:bgColor,
			borderColor: 'yellow',
			pointStyle:'triangle', 
			radius: 5, 
			pointHoverRadius: 10,
			rotation: rotation
		}
	);

}

function drawCompareChart(title, data, params) {

	let priceGubun = PriceGubun.getPriceGubun(params);

	drawNewChartCommon(title, data, params, priceGubun.title);

	ChartWrapper.addRightYAxe("거래건수", 'B');

	ChartWrapper.makeChartDataset('bar', '1년전', 'A', data['before_1y_'+priceGubun.price_name], 'red', {'backgroundColor':'red'});
	makeScatterDataset(data, '1년전 최고가', 'before_1y_', priceGubun, 'red');
	ChartWrapper.makeChartDataset('line', '1년전 거래량', 'B', data['before_1y_cnt'], 'red', {'borderColor':'red'});

	ChartWrapper.makeChartDataset('bar', '1달전', 'A', data['before_1m_'+priceGubun.price_name], 'red', {'backgroundColor':'green'});
	makeScatterDataset(data, '1달전 최고가', 'before_1m_', priceGubun, 'green');
	ChartWrapper.makeChartDataset('line', '1달전 거래량', 'B', data['before_1m_cnt'], 'red', {'borderColor':'green'});

	ChartWrapper.makeChartDataset('bar', params['to_ym'], 'A', data['cur_'+priceGubun.price_name], 'red', {'backgroundColor':'black'});
	makeScatterDataset(data, params['to_ym']+' 최고가', 'cur_', priceGubun, 'black');
	ChartWrapper.makeChartDataset('line', params['to_ym']+' 거래량', 'B', data['cur_cnt'], 'red', {'borderColor':'black'});

	ChartWrapper.chartComplete();
}


function drawSaleCompare(params) {
	
	ChartManager.setChartType(params, CHART_TYPE.COMPARE);

	let chartType = ChartManager.getChartType(params);
	let url = chartType.url(params);
	let title = chartType.title(params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		drawCompareChart(title, data, params);
		closeMessage();
	});

}

