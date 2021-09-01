var gRegionsMap = {}; //
var gRegionsArr = [];
var gDrawOptions = {};
let gChartParams = null;
var loadingApt = false;
var myChart = null;
var gChartData = new Map();
var BASE_URL;
var CHART_TYPE = {
	TIME_SERIES: {},
	RANK_REGION: {gubun: 'Region'},
	RANK_APT: {gubun: 'Apt'}
};
var CHART_RANK_TYPE = {
	'Region': CHART_TYPE.RANK_REGION,
	'Apt': CHART_TYPE.RANK_APT
};

var gCurChartType = CHART_TYPE.TIME_SERIES;

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
	sel.children().not(':first').remove();

	if (filterFunc) {
		arr = arr.slice();
		$.each(arr, function(index, item) {
			if (!filterFunc(index, item))
				delete arr[index];
		});
	}

	if (sortKey)
		arr.sort(function(a, b) { return (a[sortKey] < b[sortKey]) ? -1 : ((a[sortKey] == b[sortKey]) ? 0 : 1); });

	$.each(arr, function(idx, r) {
		if (r) { // not filtered out element
			if (contentsFunc != null)
				sel[0].innerHTML += contentsFunc(r);
			else {
				option = document.createElement("option");
				option.value = r['key'];
				option.innerHTML = r['name'];
				sel.append(option);
			}
		}
	});

}

function initRegions() {
	sel = $("#region_key1");
	constructSel(sel, gRegionsArr);
}

function clearChildSelect(sel) {
	
	constructSel(sel, {});
	if (sel.attr('id') == 'apt') {
		aptList = document.querySelector('#apt');
		aptList.value = '';
		aptLabel = document.querySelector('#aptLabel');
		aptLabel.textContent = '전체';
//		$('#danji').attr('disabled', true);
	}

	let child = sel.attr("child");
	if (!child)
		return;

	child = $('#' + child);
	clearChildSelect(child);

}

function refreshRegion(sel) {

	let child = sel.attr("child");
	child = $('#' + child);
	clearChildSelect(child);

	params = getChartParams();
	let value = sel.val();
	if (sel.attr('id').endsWith('1')) { // region_key1 
		if (value == '')
			$('#rankByAptBtn').attr('disabled', true);
		else
			$('#rankByAptBtn').attr('disabled', false);
	}
	if (value == '' || value == null) { // just clear only
		return;
	}

	let level = gRegionsMap[value]['level'];
	if (level == 3) {
		refreshApt(child);
		return;
	}
	
	let arr = gRegionsMap[value]['subregions'];

	constructSel(child, arr, 'name' /* sort by name */, function(index, item) { return item['level'] == level+1; });

}

function refreshApt(apt) {

	clearChildSelect(apt);

	var region_key2 = $("#region_key2").val();
	var region_key3 = $("#region_key3").val();
	if (region_key2 == "" || region_key3 == "")
		return;

	var aptArr = gRegionsMap[region_key3]['subregions'];

//	$('#danji').attr('disabled', false);
	var func = ($("#danji").is(":checked") 
		? function(index, data) { 
			return data['danji'] == 'Y'; // data is array of id, array of name, danji_flag
		  } 
		: null); 
	var naverLinkFunc = 
		function(data) { 
			var html = "";
			if (data['naver'] && data['naver'].length > 0) {
				html = "<li value="+data['key']+"><a href=javascript:window.open('https://new.land.naver.com/complexes/"+data['naver'][0][0]+"');>"+data['name']+"</a></li>";
			}
			return html;
		} 
	if (aptArr && aptArr.length > 0) { // already cached
		constructSel($('#apt'), aptArr, -1 /* no sort */, func, naverLinkFunc);
		return;
	}

	loadingApt = true;
	url = BASE_URL + "getApt?region_key="+region_key3;
	$.getJSON(url, function(data){
		aptArr = JSON.parse(data);
		gRegionsMap[region_key3]['subregions'] = aptArr;
		constructSel($('#apt'), aptArr, -1 /* no sort */, func, naverLinkFunc);
		const label = document.querySelector('#aptLabel');
		const options = document.querySelectorAll('.aptList > li');

		// 클릭한 옵션의 텍스트를 라벨 안에 넣음
		const handleSelect = (item) => {
  			label.parentNode.classList.remove('active');
  			label.innerHTML = item.textContent;
			item.parentNode.value = item.value;
			item.parentNode.name = item.textContent;
			$('#apt_btn').attr('disabled', item.value == '');
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
	var input = params[key];
	if (Array.isArray(input)) {
		var value = '';
		var selected = 0;
		input.forEach(function(item) {
			if (item.is(":checked") == true) {
				value += item.val();
				selected++;
			}
		});
		if (selected == input.length) // All Selected
			value = "";
		return value;
	} else if (typeof input == "string" || typeof input == "number") {
		if (key == "region_key") {
			if ($('#region_key3').val() != '') {
				params[key] = $('#region_key3').val();
				input = params[key];
			} else if ($('#region_key2').val() != '') {
			    params[key] = $('#region_key2').val();
				input = params[key];
			} else if ($('#region_key1').val() != '') {
			    params[key] = $('#region_key1').val();
				input = params[key];
			} else {
				input = "0000000000";
			}
		}
		return input;
	} else {
		// skip if no input, not checked
		if (input.val() == null || (input.attr('type') == 'checkbox' && !input.is(':checked')))
			return '';
		return input.val();
	}
}

function checkLoadingApt(input, newValue) {
	if (loadingApt) {
		window.setTimeout(checkLoadingApt, 100, input, newValue);
	} else {
		input.val(newValue).change();
	}
}

function setValueOfParam(params, key, newValue) {
	var input = params[key];
	if (Array.isArray(input)) {
		var pos = 0;
		input.forEach(function(item) {
			if(item.val() == newValue.substring(pos, pos+input.value_len)) {
				item.prop("checked", true);
			}
			pos += input.value_len;
		});
	} else if (typeof input == "string" || typeof input == "number") {
		if (key == "region_key") {
			let	region = gRegionsMap[newValue];
			let level = region['level'];
			let upper = [];
			// find uper region recursivelly upward
			for(var l = level; l > 0; l--) {
				upper.push(region['key']);
				region = gRegionsMap[region['upper']];
			}
			// set region control value downward
			for(var l = 1; l <= level; l++) { 
				let region_sel = $('#region_key'+l);
				if (region_sel.val() != upper[level-l])
					region_sel.val(upper[level-l]).change();
			}
		}
		params[key] = newValue;
	} else {
		if (key == "apt") {
			let region = gRegionsMap[$('#region_key3').val()];
			if (!region || !region['subregions'] || region['subregions'].length == 0) {
				window.setTimeout(checkLoadingApt, 100, input, newValue);
				return;
			}
		}
		input.val(newValue).change();
	}
}

function initChartParams() {
	let params = {};

	$('#chart_conditions input, #chart_conditions select, #chart_conditions ul').each(function(index, item) {
		input = $(item);
		key = input.attr('id');
		if (params[key]) {
			tmp = params[key];
			if (!Array.isArray(tmp)) {
				params[key] = [ tmp ];
				params[key].value_len = input.val().length;
			}
			params[key].push(input);
		} else
			params[key] = input;
	});

	params['orderby'] = '';
	params['page'] = '';
	params['ym'] = '';
	params['region_key'] = '0000000000';

	var map = { params: params };

	Object.keys(params).forEach(function(key) {

		var item = params[key];
		var input = item;
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

function getChartParams() {

	if (gChartParams == null) {

		gChartParams = initChartParams();
	}

	return gChartParams;
}

function changePriceGubun(gubun) {

	// $('.price_text').text(gPriceGubun[parseInt(gubun)].title);
	let priceGubun = PriceGubun.getPriceGubun(gubun);

	$('.price_text').text(priceGubun.title);

	if (gCurChartType == CHART_TYPE.TIME_SERIES)
		updateChart();
	else
		redrawRankChart();

}

function requestSaleStat() {

	var params = getChartParams();
	params['apt'] = '';

	var title = getRegionTitle(params);

	drawSaleStat(params, title);

}

function requestSaleLine() {

	var aptList = $('#apt');
	var title = aptList[0].name;
	var params = getChartParams();

	drawSaleLineChart(params, title);

}

function requestRankChart(gubun) {

	var params = getChartParams();

	// Region Rank chart's minimum level = 2
	if (gubun == 'Region') {
		if (gRegionsMap[params['region_key']]['level'] == 3)
			params['region_key3'] = '';
		params['apt'] = '';
	}

	drawRankChart(gubun, params);
}

function toggleView(anchor, divId) {
	let div = $('#'+divId);
	if(div.is(':visible')) {
		div.hide();
		$(anchor).text('▼ 보이기');
	} else {
		div.show();
		$(anchor).text('▲ 숨기기');
	}
}

function toggleYMTable(btn, target, desc) {
	var div;
	if (desc)
		div = $("#ym_table_desc_div");
	else
		div = $("#ym_table_asc_div");
	if (div.is(':visible')) {
		$('.popup').hide();
		$('#toggle_btn').text('▼ 보이기');
	} else {
		$('.popup').hide();
		var pos = btn.position();
		div.attr("target", target);
		div.css({top: (pos.top+btn.outerHeight())+'px', left: pos.left+'px', display: 'block'});
	}
}

function wrapWindowByMask() { 
	//화면의 높이와 너비를 구한다. 
	var maskHeight = $(document).height(); 
	var maskWidth = $(window).width(); 
	
	//마스크의 높이와 너비를 화면 것으로 만들어 전체 화면을 채운다. 
	$('#fade').css({ 'width': maskWidth, 'height': maskHeight }); 
} 
	
/// 화면의 중앙에 레이어띄움 
function showMessage(label = '조회') { 
	wrapWindowByMask(); 
	$("#light").css("position", "absolute"); 
	$("#light").css("top", Math.max(0, (($(window).height() - $("#light").outerHeight()) / 2) + $(window).scrollTop() - 100) + "px"); 
	$("#light").css("left", Math.max(0, (($(window).width() - $("#light").outerWidth()) / 2) + $(window).scrollLeft()) + "px"); 
	$("#task").text(label);
	$('#fade').show(); 
	$('#light').show(); 
} 

function closeMessage() { 
	$('#fade').hide(); 
	$('#light').hide(); 
}

function setErrorHandler() {
	window.onerror = function (msg, url, lineNo, columnNo, error) {
		var string = msg.toLowerCase();
	    var message = [
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
			$('.popup').hide();
			closeMessage();
		}
	});
}

function initDrawOptionsMap() {

	var params = {};

	$('#draw_options input').each(function(index, item) {
		input = $(item);
		var key = input.attr('id');
		params[key] = input;
	});

	gDrawOptions = { params: params };

	Object.keys(params).forEach(function(key) {

		var item = params[key];
		var input = item;

		Object.defineProperty(gDrawOptions, key, {
			get: function () { 
				var input = this.params[key];
				return input.is(':checked');
			},
			set: function(bool) {
				var item = this.params[key];
				item.attr('checked', bool == true);
			}
		});
	});
	Object.defineProperty(gDrawOptions, "checked", {
		set: function(bool) {
			Object.values(this.params).forEach(function(item) {
				item.prop('checked', bool);
			});
		}
	});
	Object.defineProperty(gDrawOptions, "disabled", {
		set: function(bool) {
			Object.values(this.params).forEach(function(item) {
				item.prop('disabled', bool);
			});
		}
	});

}

function makeHtml4MonthsTR(y, to_ym) {

	var html = "<tr>";
	html += "<td>" + y + "년</td>";
	for(var m = 1; m <= 12; m++) {
		var ym = (y*100 + m <= to_ym ? (y + "" + (m < 10 ? "0"+m : m)) : "");
		html += "<td class=one_ym value='" + ym + "'>" + (ym.length > 0 ? m+"월" : "") + "</td>";
	}
	html += "</tr>:"

	return html;
}

function initSelectYMTable() {

	const MIN_YY = 2006;
	const DEFAULT_PERIOD = 3;

	var today = new Date();
	var yy = today.getFullYear();
	var mm = today.getMonth()+1;
	var to_ym = yy * 100 + mm;
	// from_ym set to 3 years minus one month ago
	var from_ym = (mm == 12 ? (yy - (DEFAULT_PERIOD - 1)) * 100 + 1 : (yy - DEFAULT_PERIOD) * 100 + mm + 1);

	for(var y = MIN_YY; y < to_ym / 100; y++) {
		var html = makeHtml4MonthsTR(y, to_ym);
		$('#ym_table_asc > tbody:last').append(html);
	};
	$('input#from_ym').val(from_ym);
	$('input#to_ym').val(to_ym);
	$('input#base_ym').val(to_ym);
	for(var y = Number.parseInt(to_ym / 100); y >= MIN_YY; y--) {
		var html = makeHtml4MonthsTR(y, to_ym);
		$('#ym_table_desc > tbody:last').append(html);
	};

	$('.one_ym').click( function() {
		var div = $($(this).parents('div')[0]);
		var target = div.attr("target");
		if ($(this).attr('value') != '') {
			$('#'+target).val($(this).attr('value'));
			div.hide();
		}
	});

	$(".one_ym").hover(function(){
		if ($(this).attr('value') != '')
			$(this).css("background-color", "yellow");
    }, function(){
		$(this).css("background-color", $(this).parent().css("background-color"));
	});

}

function setPopupPosition(event, popup) { 

	var mousePosition = {}; 
	var popupPosition = {}; 
	var menuDimension = {}; 

	menuDimension.x = popup.outerWidth(); 
	menuDimension.y = popup.outerHeight(); 
	mousePosition.x = event.pageX; 
	mousePosition.y = event.pageY; 

	if (mousePosition.x + menuDimension.x > $(window).width() + $(window).scrollLeft()) { 
		popupPosition.x = Math.max(mousePosition.x - menuDimension.x, 10);
	} else { 
		popupPosition.x = mousePosition.x; 
	} 

	if (mousePosition.y + menuDimension.y > $(window).height() + $(window).scrollTop()) { 
		popupPosition.y = Math.max(mousePosition.y - menuDimension.y, 10); 
	} else { 
		popupPosition.y = mousePosition.y; 
	} 

	return popupPosition; 
} 

var colorArr = ['#000000', '#00CC00', '#00FFFF','#FFFF00','#0066FF', '#CC0000', '#660099', '#66FF00', '#CC9999', '#CC66FF'];
var curColor = 0;

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

	$('#aptSaleTable > tbody:last').empty();
	$.each(data, function(idx, r) {
		var area = parseFloat(r[1]);
		var price = parseInt(r[3].replace(/,/g,""));
		var unit_price = (price / (area/3.3)).toFixed(1);
		unit_price = unit_price.toString().replace(/(\d)(?=(?:\d{3})+(?!\d))/g, "$1,");
		var html = '<tr class=aptSaleListLine>';
		html += "<td class=aptSaleListItem style='text-align: center;'>" + r[0] + '</td>';
		html += "<td class=aptSaleListItem style='text-align: right;'>" + r[1] + ' (' + (area/3.3).toFixed(1) + '평)</td>';
		html += "<td class=aptSaleListItem style='text-align: center;'>" + r[2] + '</td>';
		html += "<td class=aptSaleListItem style='text-align: right;'>" + r[3] + '</td>';
		html += "<td class=aptSaleListItem style='text-align: right;'>" + unit_price + '</td>';
		html += "</tr>";
		$('#aptSaleTable > tbody:last').append(html);
	});
	var div = $("#aptSaleDiv");
	var pos = setPopupPosition(event, div);
	div.css({top: pos.y+'px', left: pos.x+'px', display: 'block'});
}

function addRightYAxe(label) {
	if (myChart == null) return;

	myChart['options']['scales']['yAxes'].push({
		id: 'B',
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

function removeRightYAxe() {
	if (myChart == null) return;

	myChart['options']['scales']['yAxes'].pop();
}

function hideChart(btn, index) {
	if (myChart == null || myChart.data.datasets.length <= index || index < 0)
		return;

	legend = $('#legend'+index);
	if (legend.css('text-decoration').startsWith('line-through')) {
		myChart.data.datasets[index].hidden = false;
		legend.css('text-decoration', 'none');
		btn.attr('title', '숨기기');
		btn.css({"background-image":"url(static/images/hide.png)"}); 	
	} else {
		myChart.data.datasets[index].hidden = true;
		legend.css('text-decoration', 'line-through');
		btn.attr('title', '보이기');
		btn.css({"background-image":"url(static/images/show.png)"}); 	
	}
	myChart.update();

}

function makeRankChart2TableHTML(data) {

	var html = "<tr><th>구분</th>";
	html += "<th>"+myChart.data.datasets[0].label+"</th>";
	html += "<th>"+myChart.data.datasets[1].label+"</th>";
	html += "<th>"+myChart.data.datasets[2].label+"</th>";
	html += "</tr>";
	for(var i = 0; i < data['labels'].length; i++) {
		html += "<tr>";
		html += "<td>"+data['labels'][i]+"</td>";
		html += "<td>"+myChart.data.datasets[0].data[i]+"</td>";
		html += "<td>"+myChart.data.datasets[1].data[i]+"</td>";
		html += "<td>"+myChart.data.datasets[2].data[i]+"</td>";
		html += "</tr>";
	}
	return html;
}

function getRankChartURL(gubun, params) {

	return getChartURL("getRankBy" + gubun, params);

}

function excelChart(index) {
	if (myChart == null || myChart.data.datasets.length <= index || index < 0)
		return;

	var title = myChart.data.datasets[index].label;

	var chart = gChartData.get(title);
	const priceGubun = PriceGubun.getPriceGubun(chart.params);

	var html = "<tr>";
	html += "<th>년월</th>";
	html += "<th>평균 " + priceGubun.title + "</th>";
	if (chart.data['ma']) {
		html += "<th>평균 " + priceGubun.title + "(1YR)</th>";
	}
	if (chart.data['cnt']) {
		html += "<th>거래건수</th>";
	}
	html += "</tr>";
	var labels = Object.values(myChart.data.labels);
	var data = Object.values(myChart.data.datasets[index].data);
	var volume_data = null;
	var ma_data = null;
	if (chart.data['cnt']) {
		volume_data = Object.values(chart.data['cnt']);
	}
	if (chart.data['ma']) {
		ma_data = Object.values(chart.data[priceGubun.ma_name]);
	}
	for(var i = 0; i < data.length; i++) {
		html += "<tr>";
		html += "<td>"+labels[i]+"</td>";
		html += "<td>"+(data[i]==null?"":data[i])+"</td>";
		if (chart.data['ma']) {
			html += "<td>"+(ma_data[i]==null?"":ma_data[i])+"</td>";
		}
		if (chart.data['cnt']) {
			html += "<td>"+(volume_data[i]==null?"":volume_data[i])+"</td>";
		}
		html += "</tr>";
	}
	table2excel(title, html);
}

function excelRankChart() {

	var title = myChart.data.datasets[0].label;

	chartData = gChartData.get(title);

	url = getRankChartURL(gCurChartType.gubun, chartData.params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		table2excel(title, makeRankChart2TableHTML(data) );
		closeMessage();
	});
}

function table2excel(title, table) {
	var tab_text = '<html xmlns:x="urn:schemas-microsoft-com:office:excel">';
	tab_text += '<head><meta http-equiv="content-type" content="application/vnd.ms-excel; charset=UTF-8">';
	tab_text += '<xml><x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet>'
	tab_text += '<x:Name>Chart Data</x:Name>';
	tab_text += '<x:WorksheetOptions><x:Panes></x:Panes></x:WorksheetOptions></x:ExcelWorksheet>';
	tab_text += '</x:ExcelWorksheets></x:ExcelWorkbook></xml></head><body>';
	tab_text += "<table border='1px'>";

	tab_text += table;

	tab_text += '</table></body></html>';
	var data_type = 'data:application/vnd.ms-excel';
	var ua = window.navigator.userAgent;
	var msie = ua.indexOf("MSIE ");
	var fileName = title + '.xls';
	//Explorer 환경에서 다운로드
	if (msie > 0 || !!navigator.userAgent.match(/Trident.*rv\:11\./)) {
		if (window.navigator.msSaveBlob) {
			var blob = new Blob([tab_text], {
				type: "application/csv;charset=utf-8;"
			});
			navigator.msSaveBlob(blob, fileName);
		}
	} else {
		var blob2 = new Blob([tab_text], {
			type: "application/csv;charset=utf-8;"
		});
		var filename = fileName;
		var elem = window.document.createElement('a');
		elem.href = window.URL.createObjectURL(blob2);
		elem.download = filename;
		document.body.appendChild(elem);
		elem.click();
		document.body.removeChild(elem);
	}
}

function excelSales(index, apt) {
	if (myChart == null || myChart.data.datasets.length <= index || index < 0)
		return;

	var title = myChart.data.datasets[index].label;

	showMessage();
	$.getJSON(getChartURL('getAptSale', { 'apt': apt }), function(jsonData){
		data = JSON.parse(jsonData);
		var html = "<tr>";
		html += "<th>거래일</th>";
		html += "<th>전용면적(제곱미터)</th>";
		html += "<th>전용면적(평)</th>";
		html += "<th>층</th>";
		html += "<th>거래금액(만원)</th>";
		html += "<th>평단가(만원)</th>";
		html += "</tr>";
		$.each(data, function(idx, r) {
	  		var area = parseFloat(r[1]);
	  		var price = parseInt(r[3].replace(/,/g,""));
			html += "<tr>";
			html += "<td>" + r[0] + "</td>";
			html += "<td>" + r[1] + "</td>";
			html += "<td>" + (area/3.3).toFixed(1) + "</td>";
			html += "<td>" + r[2] + "</td>";
			html += "<td>" + r[3] + "</td>";
			var unit_price = (price / (area/3.3)).toFixed(1);
			unit_price = unit_price.toString().replace(/(\d)(?=(?:\d{3})+(?!\d))/g, "$1,");
			html += "<td>" + unit_price + "</td>";
			html += "</tr>";
		});
		table2excel(title+'(거래목록)', html);
		closeMessage();
	});
}

function delChart(index) {
	if (myChart == null || myChart.data.datasets.length <= index || index < 0)
		return;

	label = myChart.data.datasets[index].label;
	chart = gChartData.get(label);
	if (chart == 'undefined')
		return;

	// 차트 데이터와 차트를 삭제한다
	gChartData.delete(myChart.data.datasets[index].label);
	myChart.data.datasets.splice(index, 1);

	// 기본 차트에 이동평균 또는 거래량 차트가 추가되어 있으면 그것도 지운다
	for(i = 0; i < myChart.data.datasets.length; i++) {
		if (myChart.data.datasets[i].label.startsWith(label)) {
			myChart.data.datasets.splice(i, 1);
			i--;
		}
	}
	$('#chartLegend').html(myChart.generateLegend()); 
	myChart.update();

}

function setLineThick(index, thick, recursive) {
	var meta = myChart.getDatasetMeta(index);
	if (!meta || !meta.controller || !meta.controller._cachedDataOpts)
		return;

	if (thick) {
		myChart.data.datasets[index].borderWidth = 2 + meta.controller._cachedDataOpts.borderWidth;
		myChart.data.datasets[index].oldColor = meta.controller._cachedDataOpts.borderColor;
		myChart.data.datasets[index].borderColor = myChart.data.datasets[index].thickColor;
	} else {
		myChart.data.datasets[index].borderWidth = 
			(meta.bar ? Chart.defaults.global.elements.rectangle.borderWidth : Chart.defaults.global.elements.line.borderWidth);
		myChart.data.datasets[index].borderColor = myChart.data.datasets[index].oldColor;
	}

	if (recursive) return;

	var label = myChart.data.datasets[index].label;
	// 기본 차트에 이동평균 또는 거래량 차트가 추가되어 있으면
	for(i = index+1; i < myChart.data.datasets.length; i++) {
		if (myChart.data.datasets[i].label.startsWith(label)) {
			setLineThick(i, thick, true);
		}
	}

	myChart.update();
}

function getChartURL(urlName, params) {

	var url = BASE_URL + urlName + "?"

	var p = params;
	if (params.params) p = params.params;

	for (const key in p) {
		var value = params[key];
		if (value == null) value = '';
		url += key + "=" + value + "&";
	}

	return url;

}

function getConditionParam(params, param, defaultValue = null) {
	
	var value = params[param];
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

function drawLegend(chart) {

	var text = []; 

	for (i = 0, j = 0; i <chart.data.datasets.length; i++) { 
		var data = gChartData.get(chart.data.datasets[i].label);
		if (data == undefined)
			continue;
		if (j > 0)
			text.push("&nbsp;|&nbsp;");
		var color = chart.data.datasets[i].backgroundColor ? chart.data.datasets[i].backgroundColor : chart.options.defaultColor;
		text.push('<span style="background-color: ' + color + ';">&nbsp;&nbsp;&nbsp;&nbsp;</span>'); 
		text.push('<span class="line" id="legend'+i+'" style="font-size:small;" onmouseover="setLineThick('+i+', true);" onmouseout="setLineThick('+i+', false);" >'); 
		text.push(chart.data.datasets[i].label+'</span>');
		text.push("<input type=button title='숨기기' class='chart-btn chart-btn-hide' onclick='hideChart($(this), "+i+");' />");
		text.push("<input type=button title='삭제' class='chart-btn chart-btn-del' onclick='delChart("+i+");' />");
		text.push("<input type=button title='엑셀' class='chart-btn chart-btn-excel' onclick='excelChart("+i+");' />");
		const apt = data.params['apt'];
		const region = data.params['region_key'];
		if (apt != undefined && apt != "" && gRegionsMap[region]['level'] == 3) {
			text.push("<input type=button title='거래내역엑셀' class='chart-btn chart-btn-excel' onclick='excelSales("+i+","+apt+");' />");
		}
		j++;
	}

	return text.join("");

}

function getChartDataAtEvent(clickedDatasetIndex) {
	var key;
	if (gCurChartType != CHART_TYPE.TIME_SERIES) // If Rank Chart
		key = myChart.data.datasets[0].label; // key is always 1st chart
	else
		// else key is clicked chart
		key = myChart.data.datasets[clickedDatasetIndex].label;

	var originChartData = gChartData.get(key);
	if (originChartData == null) {
		for(var i = clickedDatasetIndex; i >= 0; i--) {
			if (key.startsWith(myChart.data.datasets[i].label)) {
				originChartData = gChartData.get(myChart.data.datasets[i].label);
				if (originChartData != null) {
					break;
				}
			}
		}
	}

	return originChartData;
}

function dummyFunc(params, label) {
	if (loadingApt) {
		window.setTimeout(dummyFunc, 100, params, label);
	} else {
		window.setTimeout(drawSaleLineChart, 100, params, label);
	}
}

function goForSaleLineChart(chartData, label, dataIndex) {

	let base_ym = chartData.params['base_ym'];
	let years = chartData.params['years'];
	let yy = parseInt(base_ym.substr(0,4)) - parseInt(years) - 1;
	let mm = parseInt(base_ym.substr(4,2));
	if (mm == 12) {
		yy++;
		mm = 1;
	} else
		mm++;
	var from_ym = '' + yy + (mm < 10 ? "0"+mm : mm);

	chartData.params['from_ym'] = from_ym;
	chartData.params['to_ym'] = chartData.params['base_ym'];

	switch(gCurChartType.gubun) {
	case 'Region':
		drawSaleStat(chartData.params, label);
		break;
	case 'Apt':
		chartData.params['apt'] = chartData.data['apt'][dataIndex];
		window.setTimeout(dummyFunc, 100, chartData.params, label);
	}
}

function goForRankChartOrAptSaleTable(chartData, label, evt) {
	if (chartData.params['apt'] != '') {
		chartData.params['base_ym'] = label;
		$.getJSON(getChartURL('getAptSale', chartData.params), function(jsonData){
			data = JSON.parse(jsonData);
			showAptSaleTable(evt, data);
		});
	} else {
		let gubun = '';
		// If no region_key, it's region_key rank chart
		const region_key = chartData.params['region_key'];
		const region = gRegionsMap[region_key];
		chartData.params['base_ym'] = label;
		if (region['level'] < 3) {
			gubun = 'Region';
		} else {
			gubun = 'Apt';
		}
		drawRankChart(gubun, chartData.params);
	}
}

function chartClickEventHandler(evt) {
	var activePoint = myChart.getElementAtEvent(evt);

	// make sure click was on an actual point
	if (activePoint.length == 0)
		return false;

	var clickedDatasetIndex = activePoint[0]._datasetIndex;
	var clickedDataIndex = activePoint[0]._index;

	var originChartData = getChartDataAtEvent(clickedDatasetIndex);
	// leave origin data unchanged
	let chartData = originChartData;

	var label = myChart.data.labels[clickedDataIndex];


	if (gCurChartType != CHART_TYPE.TIME_SERIES) { // if Current Chart is Rank Chart
		// drill down to sub-region
		chartData.params['region_key'] = chartData.data['region_key'][clickedDataIndex];
		goForSaleLineChart(chartData, label, clickedDataIndex);
	} else {
		goForRankChartOrAptSaleTable(chartData, label, evt);
	}

	$("#aptSaleDiv").hide();
}

function createChart(title, labels, type, legendCallbackFunc, yLabel) {
	resetChartColor();

	var ctx = document.getElementById("myChart").getContext("2d");
	myChart = new Chart (ctx, {
		type: type,
		data: {
			labels : labels,
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
	if (legendCallbackFunc) {
		myChart.options['legend'] = false;
		myChart.options['legendCallback'] = legendCallbackFunc;
	}

	document.getElementById("myChart").onclick = chartClickEventHandler;
}

function clearChart() {
	if (myChart != null) {
		myChart.destroy();
		myChart = null;
	}
	gChartData.clear();
	gCurChartType = CHART_TYPE.TIME_SERIES; // default chart type

}

function getMonths(ym1, ym2) {
	var min_ym, max_ym;
	if (ym1 < ym2) {
		min_ym = ym1;
		max_ym = ym2;
	} else {
		min_ym = ym2;
		max_ym = ym1;
	}
	var years = parseInt(max_ym.substr(0, 4)) - parseInt(min_ym.substr(0, 4));
	var months = parseInt(max_ym.substr(4, 2)) - parseInt(min_ym.substr(4, 2));
	return years * 12 + months;

}

function lpadData(ym1, ym2, data, volume_data) {

	var months = getMonths(ym1, ym2);
	for(var i = 0; i < months; i++) {
		data.unshift(null);
		if (volume_data)
			volume_data.unshift(null);
	}

}

function rpadData(ym1, ym2, data, volume_data) {

	var months = getMonths(ym1, ym2);
	for(var i = 0; i < months; i++) {
		data.push(null);
		if (volume_data)
			volume_data.unshift(null);
	}
}

function lpadLabels(minYm, labels) {
	while (labels[0] > minYm) {
		var year = parseInt(labels[0].substr(0, 4));
		var mm = parseInt(labels[0].substr(4, 2));
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
		var year = parseInt(labels[labels.length-1].substr(0, 4));
		var mm = parseInt(labels[labels.length-1].substr(4, 2));
		mm = mm + 1;
		if (mm > 12) {
			year = year + 1;
			mm = 1;
		}
		labels.push(year.toString() + (mm < 10 ? "0"+mm.toString() : mm.toString()));
	}
}

function spreadLabels(labels, data, volume_data) {
    var chartLabels = myChart['data']['labels'];
    if (chartLabels[0] < labels[0]) {
      lpadData(chartLabels[0], labels[0], data, volume_data);
    } else if (chartLabels[0] > labels[0]) {
      var chartDatasets = myChart['data']['datasets'];
      for(var i = 0; i < chartDatasets.length; i++) {
        lpadData(chartLabels[0], labels[0], chartDatasets[i].data);
      }
      lpadLabels(labels[0], chartLabels);
    }
    if (chartLabels[chartLabels.length-1] > labels[labels.length-1]) {
      rpadData(chartLabels[chartLabels.length-1], labels[labels.length-1], data, volume_data);
    } else if (chartLabels[chartLabels.length-1] < labels[labels.length-1]) {
      var chartDatasets = myChart['data']['datasets'];
      for(var i = 0; i < chartDatasets.length; i++) {
        rpadData(chartLabels[chartLabels.length-1], labels[labels.length-1], chartDatasets[i].data);
      }
      rpadLabels(labels[labels.length-1], chartLabels);
    }
}

function updateChart() {
	if (myChart == null) return;

	// 현재 그려져있는 차트 데이터를 지우고
	while(myChart['data']['datasets'].length > 0)
		myChart['data']['datasets'].pop();

	if (gDrawOptions['draw_volume'] && myChart['options']['scales']['yAxes'].length == 1)
		addRightYAxe('거래건수');

	if (!gDrawOptions['draw_volume'] && myChart['options']['scales']['yAxes'].length > 1)
		removeRightYAxe();

	// 보관하고 있는 원래의 데이터로 다시 차트를 그린다
	for(let key of gChartData.keys()) {
		var chart = gChartData.get(key);
		drawChart(key, chart.labels, $.extend(true, {}, chart.data), chart.params, chart.chartOptions);
	}
}

function makeRelativeData(base_pos, data) {

	// 그 데이터를 기준(100)으로 다 상대 수치로 변환한다
	var base = data[base_pos];
	for (var i = 0; i < data.length; i++) {
		if (data[i] != null) 
			data[i] = ((data[i] / base) * 100).toFixed(1);
	}
}

function makeChartDataset(chartType, title, yAxisID, data, color, additionalParams) {

	var dataset = {
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

	myChart['data']['datasets'].push(dataset);

}


function setDataRelative(mainData, maData) {
	// null이 아닌 첫번째 데이터를 찾아서
	var base_pos = 0;
	while (true) {
		if (mainData[base_pos] == null && base_pos < mainData.length)
			base_pos++;
		else
			break;
	}
	if (base_pos == mainData.length) 
		base_pos--;
	makeRelativeData(base_pos, mainData);
	if (gDrawOptions['draw_ma'] && maData != null)
		makeRelativeData(base_pos, maData);
}

function drawChart(title, labels, data, params, chartOptions){

	//var priceGubun = gPriceGubun[parseInt(params['price_gubun'])];
	const priceGubun = PriceGubun.getPriceGubun(params);
	const mainData = data[priceGubun.price_name];
	const volumeData = data['cnt'];
	const maData = data[priceGubun.ma_name];

	// 상대수치로 그리기 라면
	if (gDrawOptions['draw_relative']) {
		setDataRelative(mainData, maData);
	}

	if (!chartOptions)
		chartOptions = {};

	if (!chartOptions['backgroundColor']) {
		var bgColor = Chart.defaults.global.defaultColor;
		if (gDrawOptions['draw_overlap'])
			bgColor = getNextChartColor();
		chartOptions['backgroundColor'] = bgColor;
	}

	makeChartDataset('line', title, 'A', mainData, 'red', chartOptions);

	// 거래량 같이 그리기
	if (gDrawOptions['draw_volume'] && volumeData) {
		if (myChart['options']['scales']['yAxes'].length > 1)
			removeRightYAxe();
		addRightYAxe('거래건수');

		makeChartDataset('bar', title + "(거래건수)", 'B', volumeData, 'yellow', chartOptions);
	}

	// 6개월 이동평균으로 그리기
	if (gDrawOptions['draw_ma'] && maData != null) {
		chartOptions['borderDash'] = [5, 15];
		makeChartDataset('line', title + "(1YR)", 'A', maData, 'blue', chartOptions);
		delete chartOptions['borderDash'];
	}

	myChart.config.options.scales.yAxes[0].scaleLabel.labelString = priceGubun.title;

	$('#chartLegend').html(myChart.generateLegend()); 

	myChart.update();

}

function drawNewChart(title, labels, data, params){

	//var priceGubun = gPriceGubun[parseInt(params['price_gubun'])];
	const priceGubun = PriceGubun.getPriceGubun(params);
	const mainData = data[priceGubun.price_name];
	const volumeData = data['cnt'];
	const maData = data[priceGubun.ma_name];
	const yLabel = priceGubun.title;

	gDrawOptions.disabled = false;

	// 팡업 화면을 숨긴다
	$("#aptSaleDiv").hide();

	// 겹쳐 그리기 옵션이 아니면 기존 차트를 지운다
	if (!gDrawOptions['draw_overlap']) {
		clearChart();
	}

	// 차트가 없다면 생성한다
	if (myChart == null) {
		let func = function(chart) { return drawLegend(chart); };
		createChart(title, labels, 'line', func, yLabel);
	}

	// 기존에 그려진 차트가 있으면
	if (myChart['data']['datasets'].length > 0) {
		// 새 데이터와 기존 데이터의 X 라벨의 From~To를 맞춰준다
		spreadLabels(labels, mainData, volumeData);
	}

	var copy_params = {};
	for (var key in params) {
		var item = params[key];
		if (typeof(item) != "object")
			copy_params[key] = item;
	}

	gChartData.set(title, {
		data: $.extend({}, data),
		labels: $.extend({}, labels),
		params: copy_params
	});

	// 차트용 데이터를 구성하고 차트를 그린다
	drawChart(title, labels, data, params);

}

function drawSaleLineChart(params, title) {

	const apt = params['apt']
	if (apt == undefined || apt == "") {
		alert('아파트가 선택되지 않았습니다.');
		return;
	}
	if (title == undefined || title == "") {
		alert('아파트가 선택되지 않았습니다.');
		return;
	}

	title = makeChartConditionTitle(title, params, {'danji':true, 'ages':true, 'age_sign':true});

	if (gDrawOptions['draw_overlap'] && Array.from(gChartData.keys()).includes(title)) {
		alert("이미 있는 차트입니다.");
		return;
	}

	var url = getChartURL('getSale', params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		drawNewChart(title, data['labels'], data, params);
		closeMessage();
	});

}

function makeChartConditionTitle(title, params, toIgnoreParams = []) {

	// check if any param exists
	var existsParams = false;
	for(const key in params.params) {
		if (key == 'region_key')
			continue;
		if (params[key] != '' && !toIgnoreParams[key]) {
			existsParams = true;
			break;
		}
	}

	// if no param, just return title
  	if (!existsParams)
  		return title;

	var needsComma = false;
    title += " (";
	if (!toIgnoreParams['danji']) {
		const danji = getConditionParam(params, 'danji');
    	if (danji == "Y"){
      		title += "등록된 단지만";
    	} else {
      		title += "모든 실거래";
    	}
		needsComma = true;
	}
	if (!toIgnoreParams['from_ym'] && !toIgnoreParams['to_ym']) {
		const from_ym = getConditionParam(params, 'from_ym');
		const to_ym = getConditionParam(params, 'to_ym');
    	if (from_ym + to_ym != ""){
			if (needsComma) title += ", ";
			else needsComma = true;
      		title += "기간 : " + from_ym + " ~ " + to_ym;
    	}
	}
	if (!toIgnoreParams['ages'] && !toIgnoreParams['age_sign']) {
		const ages = getConditionParam(params, 'ages');
		const age_sign = getConditionParam(params, 'age_sign');
    	if (ages != '' && age_sign != ''){
			if (needsComma) title += ", ";
			else needsComma = true;
      		title += "연식 : " + ages + " 년 " + (age_sign == '<' ? "이내" : "이상");
    	}
	}
	if (!toIgnoreParams['area_type']) {
		const area_type = getConditionParam(params, 'area_type');
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
    title += ")";
 
 	return title;

}

function drawSaleStat(params, title) {
	
	title = makeChartConditionTitle(title, params);

	if (gDrawOptions['draw_overlap'] && Array.from(gChartData.keys()).includes(title)) {
		alert("이미 있는 차트입니다.");
		return;
	}

	var url = getChartURL('getSaleStat', params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		drawNewChart(title, data['labels'], data, params);
		closeMessage();
	});
}

function drawRankLegend(chart) {

	var text = [];

	var chartData = gChartData.get(chart.data.datasets[0].label);
	var chartParams = chartData['params'];

	text.push('<center>');
	if (chartParams.hasPrior)
		text.push("<span><a onclick='drawRankChartPage(false);'>◀&nbsp;&nbsp;</a></span>");

	for (i = 0; i <chart.data.datasets.length; i++) { 
		if (i > 0)
			text.push("&nbsp;|&nbsp;");
		var color = chart.data.datasets[i].backgroundColor ? chart.data.datasets[i].backgroundColor : chart.options.defaultColor;
		text.push('<span style="background-color: ' + color + ';">&nbsp;&nbsp;&nbsp;&nbsp;</span>'); 
		text.push('<span style="font-size:small;">' +chart.data.datasets[i].label + '</span>');
	}
	text.push("&nbsp;|&nbsp;");
	var curOrderby = chartParams['orderby'];
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
	
	chartData = gChartData.get(myChart.data.datasets[0].label);

	var selectedIndex = $('#rank_orderby option').index($('#rank_orderby option:selected'));
	const priceGubun = PriceGubun.getPriceGubun(chartData.params);
	const orderByOptions = priceGubun.getOrderByArr();
	chartData.params['orderby'] = orderByOptions[selectedIndex].var_name;

	chartData.params['page'] = 1;

	drawRankChart(gCurChartType.gubun, chartData.params);

}

function drawRankChartPage(next) {

	
	chartData = gChartData.get(myChart.data.datasets[0].label);
	var page = chartData.params['page'];
	if (next)
		page += 1;
	else
		page -= 1;
	chartData.params['page'] = page;

	drawRankChart(gCurChartType.gubun, chartData.params);
		
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

function makeTitle4RankChart(gubun, params) {
	let title = getRegionTitle(params);
	const priceGubun = PriceGubun.getPriceGubun(params);

	switch(gubun) {
		case 'Region':
			title += '지역별 ' + priceGubun.title + ' 변동율';
			break;
		case 'Apt':
			title += '아파트별 ' + priceGubun.title + ' 변동율';
			break;
	}

	var ignoreParams = {'from_ym':true, 'to_ym':true, 'orderby':true, 'page':true};
	title = makeChartConditionTitle(title, params, ignoreParams );

	return title;
}

function getTooltip4RankChart(tooltipItem, data) {
	if (tooltipItem.length != 1) return;
	if (tooltipItem[0].datasetIndex != 1 && tooltipItem[0].datasetIndex != 2) return;
	let dsIndex = tooltipItem[0].datasetIndex % 2 + 1;
	let ds = myChart.data.datasets[dsIndex];
	let body = myChart.data.datasets[tooltipItem[0].datasetIndex].label + ": " + tooltipItem[0].value + " / " + ds.label + ": " + ds.data[tooltipItem[0].index];
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

function setXAxeForDualBar() {
	myChart.options.scales['xAxes'][0]['stacked'] = true;
	myChart.options.scales['xAxes'][0]['offset'] = true;
	myChart.options.scales['xAxes'][0]['gridLines'] = { offsetGridLines: true };
	myChart.options.scales['xAxes'].push({
        stacked: true,
		display: false,
	    id: "X2",
	    gridLines: {
		    offsetGridLines: true
		},
    	offset: true
   	});
}

function initParamAndOption4RankChart(params) {
	gDrawOptions.checked = false;
	gDrawOptions.disabled = true;

	const priceGubun = PriceGubun.getPriceGubun(params);
	const orderByOptions = priceGubun.getOrderByArr();
	// if not exists, add & set default value for params - orderby & page
	getConditionParam(params, 'orderby', orderByOptions[0].var_name);
	let page = getConditionParam(params, 'page', 1);
}

function drawRankChart(gubun, params) {
	
	initParamAndOption4RankChart(params);

	let url = getRankChartURL(gubun, params);
	let title = makeTitle4RankChart(gubun, params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);

		$("#aptSaleDiv").hide(); // 팡업 화면을 숨긴다

		clearChart();
		createChart(title, data['labels'], 'line', function(chart) { return drawRankLegend(chart); }, '변동율(%)');

		gCurChartType = CHART_RANK_TYPE[gubun]; // set current chart to Rank chart
		myChart.options.tooltips.callbacks = {
        	beforeBody: getTooltip4RankChart
		};

		setPageNavigator(params, data);

		gChartData.set(title, { 'data': data, 'params': params });

		//const priceGubun = gPriceGubun[parseInt(params['price_gubun'])];
		const priceGubun = PriceGubun.getPriceGubun(params);
		let price_name = priceGubun.price_name;
		let rate_name = priceGubun.rate_name;
		let yLabel = priceGubun.title;
		let bprice_name = priceGubun.before_price_name;

		setXAxeForDualBar();
		addRightYAxe(yLabel);

		makeChartDataset('line', title, 'A', data[rate_name]);
		makeChartDataset('bar', params['base_ym'] + '의 ' + yLabel, 'B', data[price_name], null, { backgroundColor : 'rgba(0, 255, 0, 0.5)', barPercentage: 0.9 });
		makeChartDataset('bar', params['years'] + '년 전의' + yLabel, 'B', data[bprice_name], null, { backgroundColor : 'rgba(255, 0, 0, 1)', xAxisID: 'X2', barPercentage: 0.5 });

		$('#chartLegend').html(myChart.generateLegend()); 
		myChart.update();
		
		closeMessage();

	});
}

function initCustomSelect() {

	const label = document.querySelector('#aptLabel');

	// 라벨을 클릭시 옵션 목록이 열림/닫힘
	label.addEventListener('click', () => {
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

