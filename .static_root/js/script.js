var gRegionsMap = {}; //
var gCookie_Region = "last_region";
var gRegionsArr = [];
var gDrawOptions = {};
let gChartParams = null;
var loadingApt = false;
var gMainChart = null;
var gChartData = new Map();
var BASE_URL;
var CHART_TYPE = {
	NONE: -1,
	TIME_SERIES: {},
	RANK_REGION: {gubun: 'Region'},
	RANK_APT: {gubun: 'Apt'},
	COMPARE: {gubun: 'Compare'}
};
var CHART_RANK_TYPE = {
	'Region': CHART_TYPE.RANK_REGION,
	'Apt': CHART_TYPE.RANK_APT
};

var gCurChartType = CHART_TYPE.NONE;

let YAXE_POSITION = { 
	LEFT : 1,
	RIGHT : 2
};

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
	switch(level) {
	case 1:
		setCookie(gCookie_Region, value, 100);
		break;
	case 2:
		setCookie(gCookie_Region, $('#region_key1').val()+"|"+value, 100);
		break;
	case 3:
		setCookie(gCookie_Region, $('#region_key1').val()+"|"+$('#region_key2').val()+"|"+value, 100);
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
	var container = document.getElementById('map'); //지도를 담을 영역의 DOM 레퍼런스
	var callback = function(result, status) {
    	if (status === kakao.maps.services.Status.OK) {
			container.style.display = 'block';
    		// 지도 중심을 이동 시킵니다
			var options = { //지도를 생성할 때 필요한 기본 옵션
				center: new kakao.maps.LatLng(result[0].y, result[0].x),
				level: 3 //지도의 레벨(확대, 축소 정도)
			};

			map = new kakao.maps.Map(container, options); //지도 생성 및 객체 리턴
			var marker = new kakao.maps.Marker({
        		position: new kakao.maps.LatLng(result[0].y, result[0].x),
        		text: label     
    		});
			marker.setMap(map);

			var infowindow = new kakao.maps.InfoWindow({
    			position : marker.position,
    			content : '<div style="padding:5px;"><center>' + label + '</center></div>'
			});

			infowindow.open(map, marker);
    	}
	};

	geocoder = new kakao.maps.services.Geocoder();
	geocoder.addressSearch(juso, callback);

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
			var html = "<li value="+data['key']+">";
			if (data['naver']['id'] > 0) {
				const naver_land = "https://new.land.naver.com/complexes/";
				html += "<a href=javascript:window.open('" + naver_land + data['naver']['id'] + "');>"
				html += data['name']+"</a>";
				html += "&nbsp;&nbsp;<a href=\"javascript:showMap('" 
					 + data['naver']['road_addr'] + "', '" + data['naver']['name'] + "');\">map</a>";
			} else {
				html += data['name'];
			}
			html += "</li>";
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
			let name = item.textContent;
			name = name.endsWith("map") ? name.substring(0, name.length-3) : name; 
  			label.innerHTML = name;
			item.parentNode.value = item.value;
			item.parentNode.name = name;
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
		let default_val = input.attr('default');
		if (!default_val) default_val = '';
		let val = input.val();
		if (!val) val = '';
		if (input.attr('type') == 'checkbox' && !input.is(':checked'))
			return default_val;
		return val;
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
		params[key] = newValue;
		let cur_val = input.val();
		if (cur_val != newValue) {
			let sel = input.val(newValue);
			sel.change();
		}
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
	
	if (!gMainChart) return;

	for(var i = 0; i < gMainChart['data']['datasets'].length; i++) {
		let key = gMainChart['data']['datasets'][i].label;
		chart = gChartData.get(key);
		if (!chart) continue;

		chart.params['price_gubun'] = gubun;
	}

	$('.price_text').text(priceGubun.title);

	switch(gCurChartType) {
	case CHART_TYPE.TIME_SERIES:
		updateChart();
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

	var params = getChartParams();
	params['apt'] = '';

	var title = "월별추이" + getRegionTitle(params);

	drawSaleStat(params, title);

}

function requestSaleLine() {

	var aptList = $('#apt');
	var title = "월별추이[" + aptList[0].name + "]";
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

function requestSaleCompare() {

	var params = getChartParams();
	params['apt'] = '';

	var title = "지역별 비교" + getRegionTitle(params);

	drawSaleCompare(params, title);

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
			enumerable: true,
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

function addRightYAxe(label, id) {
	if (gMainChart == null) return;

	if (gMainChart['options']['scales']['yAxes'].length > 1)
		gMainChart['options']['scales']['yAxes'].pop();

	gMainChart['options']['scales']['yAxes'].push({
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

function hideChart(btn, index) {
	if (gMainChart == null || gMainChart.data.datasets.length <= index || index < 0)
		return;

	legend = $('#legend'+index);
	if (legend.css('text-decoration').startsWith('line-through')) {
		gMainChart.data.datasets[index].hidden = false;
		legend.css('text-decoration', 'none');
		btn.attr('title', '숨기기');
		btn.css({"background-image":"url(static/images/hide.png)"}); 	
	} else {
		gMainChart.data.datasets[index].hidden = true;
		legend.css('text-decoration', 'line-through');
		btn.attr('title', '보이기');
		btn.css({"background-image":"url(static/images/show.png)"}); 	
	}
	gMainChart.update();

}

function makeRankChart2TableHTML(data) {

	var html = "<tr><th>구분</th>";
	html += "<th>"+gMainChart.data.datasets[0].label+"</th>";
	html += "<th>"+gMainChart.data.datasets[1].label+"</th>";
	html += "<th>"+gMainChart.data.datasets[2].label+"</th>";
	html += "</tr>";
	for(var i = 0; i < data['labels'].length; i++) {
		html += "<tr>";
		html += "<td>"+data['labels'][i]+"</td>";
		html += "<td>"+gMainChart.data.datasets[0].data[i]+"</td>";
		html += "<td>"+gMainChart.data.datasets[1].data[i]+"</td>";
		html += "<td>"+gMainChart.data.datasets[2].data[i]+"</td>";
		html += "</tr>";
	}
	return html;
}

function getRankChartURL(gubun, params) {

	return getChartURL("getRankBy" + gubun, params);

}

function excelChart(index) {
	if (gMainChart == null || gMainChart.data.datasets.length <= index || index < 0)
		return;

	var title = gMainChart.data.datasets[index].label;

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
	var labels = Object.values(gMainChart.data.labels);
	var data = Object.values(gMainChart.data.datasets[index].data);
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

	var title = gMainChart.data.datasets[0].label;

	let chartData = gChartData.get(title);

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
	if (gMainChart == null || gMainChart.data.datasets.length <= index || index < 0)
		return;

	var title = gMainChart.data.datasets[index].label;

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
	if (gMainChart == null || gMainChart.data.datasets.length <= index || index < 0)
		return;

	let label = gMainChart.data.datasets[index].label;
	chart = gChartData.get(label);
	if (!chart)
		return;

	let sel = document.getElementById("chartHist");
	let opt = sel.options[sel.selectedIndex];
	let arr = opt.value.split(":");
	opt.value = "";
	for (var i = 0; i < arr.length; i++) {
		if (arr[i] == label) {
			arr.splice(i, 1);
			i--;
		} else {
			opt.value += ":" + arr[i];
		}
	}
	gChartData.delete(label);
	gMainChart.data.datasets.splice(index, 1);

	// 기본 차트에 이동평균 또는 거래량 차트가 추가되어 있으면 그것도 지운다
	for(i = 0; i < gMainChart.data.datasets.length; i++) {
		if (gMainChart.data.datasets[i].label.startsWith(label)) {
			gMainChart.data.datasets.splice(i, 1);
			i--;
		}
	}

	$('#chartLegend').html(gMainChart.generateLegend()); 
	gMainChart.update();

}

function setLineThick(index, thick, recursive) {
	var meta = gMainChart.getDatasetMeta(index);
	if (!meta || !meta.controller || !meta.controller._cachedDataOpts)
		return;

	if (thick) {
		gMainChart.data.datasets[index].borderWidth = 2 + meta.controller._cachedDataOpts.borderWidth;
		gMainChart.data.datasets[index].oldColor = meta.controller._cachedDataOpts.borderColor;
		gMainChart.data.datasets[index].borderColor = gMainChart.data.datasets[index].thickColor;
	} else {
		gMainChart.data.datasets[index].borderWidth = 
			(meta.bar ? Chart.defaults.global.elements.rectangle.borderWidth : Chart.defaults.global.elements.line.borderWidth);
		gMainChart.data.datasets[index].borderColor = gMainChart.data.datasets[index].oldColor;
	}

	if (recursive) return;

	var label = gMainChart.data.datasets[index].label;
	// 기본 차트에 이동평균 또는 거래량 차트가 추가되어 있으면
	for(i = index+1; i < gMainChart.data.datasets.length; i++) {
		if (gMainChart.data.datasets[i].label.startsWith(label)) {
			setLineThick(i, thick, true);
		}
	}

	gMainChart.update();
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
		if (!data)
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
	switch(gCurChartType) {
	case CHART_TYPE.TIME_SERIES:
		key = gMainChart.data.datasets[clickedDatasetIndex].label;
		break;
	case CHART_TYPE.RANK_REGION:
	case CHART_TYPE.RANK_APT:
		key = gMainChart.data.datasets[0].label; // key is always 1st chart
		break;
	case CHART_TYPE.COMPARE:
		return null;
	}

	var originChartData = gChartData.get(key);
	if (originChartData == null) {
		for(var i = clickedDatasetIndex; i >= 0; i--) {
			if (key.startsWith(gMainChart.data.datasets[i].label)) {
				originChartData = gChartData.get(gMainChart.data.datasets[i].label);
				if (originChartData) {
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
	var activePoint = gMainChart.getElementAtEvent(evt);

	// make sure click was on an actual point
	if (activePoint.length == 0)
		return false;

	var clickedDatasetIndex = activePoint[0]._datasetIndex;
	var clickedDataIndex = activePoint[0]._index;

	var originChartData = getChartDataAtEvent(clickedDatasetIndex);
	// leave origin data unchanged
	let chartData = originChartData;

	var label = gMainChart.data.labels[clickedDataIndex];

	switch (gCurChartType) {
		case CHART_TYPE.TIME_SERIES:
			goForRankChartOrAptSaleTable(chartData, label, evt);
			break;
		case CHART_TYPE.COMPARE:
			break;
		default:
			// drill down to sub-region
			chartData.params['region_key'] = chartData.data['region_key'][clickedDataIndex];
			goForSaleLineChart(chartData, label, clickedDataIndex);
	}

	$("#aptSaleDiv").hide();
}

function setChartLegendFunc(legendCallbackFunc) {

	if (legendCallbackFunc) {
		gMainChart.options['legend'] = false;
		gMainChart.options['legendCallback'] = legendCallbackFunc;
	}
}

function createChart(title, data, params, type, yLabel, chartType) {
	resetChartColor();

	var ctx = document.getElementById("myChart").getContext("2d");
	gMainChart = new Chart (ctx, {
		type: type,
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

	gCurChartType = chartType;

	if (gCurChartType == CHART_TYPE.TIME_SERIES) {
		gDrawOptions.disabled = false;
	} else {
		gDrawOptions.checked = false;
		gDrawOptions.disabled = true;
	}
}

function clearChart() {

	if (gMainChart != null) {
		gMainChart.destroy();
		gMainChart = null;
	}
	//gChartData.clear();

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
	var chartLabels = gMainChart['data']['labels'];
	if (chartLabels[0] < labels[0]) {
		lpadData(chartLabels[0], labels[0], data, volume_data);
	} else if (chartLabels[0] > labels[0]) {
		var chartDatasets = gMainChart['data']['datasets'];
		for(var i = 0; i < chartDatasets.length; i++) {
			lpadData(chartLabels[0], labels[0], chartDatasets[i].data);
		}
		lpadLabels(labels[0], chartLabels);
	}
	if (chartLabels[chartLabels.length-1] > labels[labels.length-1]) {
		rpadData(chartLabels[chartLabels.length-1], labels[labels.length-1], data, volume_data);
	} else if (chartLabels[chartLabels.length-1] < labels[labels.length-1]) {
		var chartDatasets = gMainChart['data']['datasets'];
		for(var i = 0; i < chartDatasets.length; i++) {
			rpadData(chartLabels[chartLabels.length-1], labels[labels.length-1], chartDatasets[i].data);
		}
		rpadLabels(labels[labels.length-1], chartLabels);
	}
}

function updateChart() {
	if (gMainChart == null) return;

	while(gMainChart['data']['datasets'].length > 0)
		gMainChart['data']['datasets'].pop();

	// 보관하고 있는 원래의 데이터로 다시 차트를 그린다

	for(let key of gChartData.keys()) {
		var chart = gChartData.get(key);
		drawChart(key, chart.data, chart.params);
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

	gMainChart['data']['datasets'].push(dataset);

}


function setDataRelative(data) {
	// null이 아닌 첫번째 데이터를 찾아서
	var base_pos = 0;
	while (true) {
		if (data[base_pos] == null && base_pos < data.length)
			base_pos++;
		else
			break;
	}
	if (base_pos == data.length) 
		base_pos--;

	makeRelativeData(base_pos, data);

	return base_pos;
}

/*
	chartData = { 'label', 'type', 'data', yAxe { id, direction, label }, color, chartOptions }
*/
function drawChart0(title, data, priceGubun, chartOptions = {}){

	let base_pos = -1;
	for (let i = 0; i < data.length; i++) {
		let chartOptions = data[i].chartOptions;
		if (!chartOptions)
			chartOptions = {};

		if (!chartOptions['backgroundColor']) {
			var bgColor = Chart.defaults.global.defaultColor;
			if (gDrawOptions['draw_overlap'])
				bgColor = getNextChartColor();
			chartOptions['backgroundColor'] = bgColor;
		}

		if (data[i].yAxe.position == YAXE_POSITION.RIGHT && gMainChart['options']['scales']['yAxes'].length == 1)
			addRightYAxe(data[i].yAxe.label, data[i].yAxe.id);
		makeChartDataset(data[i].type, title, data[i].yAxe.id, data[i].data, data[i].color, chartOptions);

	}

	gMainChart.config.options.scales.yAxes[0].scaleLabel.labelString = priceGubun.title;

	$('#chartLegend').html(gMainChart.generateLegend()); 

	gMainChart.update();

}

function drawChart(title, data, params){

	let map = document.getElementById("map");
	if (map)
		map.style.display = 'none';

	const priceGubun = PriceGubun.getPriceGubun(params);

	let chartData = [];
	let subChart = {};
	subChart['data'] = data[priceGubun.price_name];

	let base_pos = -1;
	// 상대수치로 그리기 라면
	if (gDrawOptions['draw_relative']) 
		base_pos = setDataRelative(subChart['data']);
	subChart['type'] = 'line';
	subChart['label'] = title;
	subChart['color'] = 'red';
	subChart['yAxe'] = { 'position': YAXE_POSITION.LEFT, 'id': 'A' };
	chartData.push(subChart);

	// 거래량 같이 그리기
	if (gDrawOptions['draw_volume'] && data['cnt']) {
		subChart = {};
		subChart['data'] = data['cnt'];
		if (gDrawOptions['draw_relative']) 
			makeRelativeData(base_pos, subChart['data']);
		subChart['type'] = 'bar';
		subChart['label'] = title + "(거래건수)";
		subChart['color'] = 'yellow';
		subChart['yAxe'] = { 'position': YAXE_POSITION.RIGHT, 'id': 'B', 'label': '거래건수' };
		chartData.push(subChart);
	}

	// 6개월 이동평균으로 그리기
	if (gDrawOptions['draw_ma'] && data[priceGubun.ma_name]) {
		subChart = {};
		subChart['data'] = data[priceGubun.ma_name];
		if (gDrawOptions['draw_relative']) 
			makeRelativeData(base_pos, subChart['data']);
		subChart['type'] = 'line';
		subChart['label'] = title + "(1YR)";
		subChart['color'] = 'blue';
		subChart['yAxe'] = { 'position': YAXE_POSITION.LEFT, 'id': 'A' };
		chartData.push(subChart);
	}

	drawChart0 (title, chartData, priceGubun);
	gMainChart.config.options.scales.yAxes[0].scaleLabel.labelString = priceGubun.title;

	$('#chartLegend').html(gMainChart.generateLegend()); 

	document.getElementById("myChart").onclick = chartClickEventHandler;


	gMainChart.update();
}

function drawNewChartCommon(title, data, params, type, yLabel, chartType, legendFunc) {

	// 팡업 화면을 숨긴다
	$("#aptSaleDiv").hide();

	let sel = document.getElementById("chartHist");
	if (gDrawOptions['draw_overlap']) {
    	let option = sel.options[sel.selectedIndex];
    	option.innerHTML = "월별추이[복합]";
		option.value += "$" + title;
	} else {
		// 겹쳐 그리기 옵션이 아니면 기존 차트를 지운다
		clearChart();

		// 기존에 있던 차트면 select에서 삭제
		let chart = gChartData.get(title);
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
		sel.append(option);
	}

	// 차트가 없다면 생성한다
	if (gMainChart == null) {
		createChart(title, data, params, type, yLabel, chartType);
		setChartLegendFunc(legendFunc);
	}

	var copy_params = {};
	for (var key in params) {
		var item = params[key];
		if (typeof(item) != "object")
			copy_params[key] = item;
	}

	gChartData.set(title, {
		type: chartType,
		data: $.extend({}, data),
		params: copy_params
	});
	
}

function drawNewChart(title, data, params){

	const priceGubun = PriceGubun.getPriceGubun(params);
	const mainData = data[priceGubun.price_name];
	const volumeData = data['cnt'];
	const yLabel = priceGubun.title;

	drawNewChartCommon(title, data, params, 'line', yLabel, CHART_TYPE.TIME_SERIES, drawLegend);

	// 기존에 그려진 차트가 있으면
	if (gMainChart['data']['datasets'].length > 0) {
		// 새 데이터와 기존 데이터의 X 라벨의 From~To를 맞춰준다
		spreadLabels(data['labels'], mainData, volumeData);
	}
	
	// 차트용 데이터를 구성하고 차트를 그린다
	drawChart(title, data, params, true);

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
		drawNewChart(title, data, params);
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
   			title += "300세대 이상만";
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
	if (!toIgnoreParams['base_ym'] && !toIgnoreParams['years']) {
		const base_ym = getConditionParam(params, 'base_ym');
		const years = getConditionParam(params, 'years');
    	if (base_ym != "" && years != ""){
			if (needsComma) title += ", ";
			else needsComma = true;
			title += "기준년월 : " + base_ym + ", 비교대상 : " + years + "년 전"; 
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
		drawNewChart(title, data, params);
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
	
	chartData = gChartData.get(gMainChart.data.datasets[0].label);

	var selectedIndex = $('#rank_orderby option').index($('#rank_orderby option:selected'));
	const priceGubun = PriceGubun.getPriceGubun(chartData.params);
	const orderByOptions = priceGubun.getOrderByArr();
	chartData.params['orderby'] = orderByOptions[selectedIndex].var_name;

	chartData.params['page'] = 1;

	drawRankChart(gCurChartType.gubun, chartData.params);

}

function drawRankChartPage(next) {

	
	chartData = gChartData.get(gMainChart.data.datasets[0].label);
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
	let ds = gMainChart.data.datasets[dsIndex];
	let body = gMainChart.data.datasets[tooltipItem[0].datasetIndex].label + ": " + tooltipItem[0].value + " / " + ds.label + ": " + ds.data[tooltipItem[0].index];
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
	gMainChart.options.scales['xAxes'][0]['stacked'] = true;
	gMainChart.options.scales['xAxes'][0]['offset'] = true;
	gMainChart.options.scales['xAxes'][0]['gridLines'] = { offsetGridLines: true };
	gMainChart.options.scales['xAxes'].push({
        stacked: true,
		display: false,
	    id: "X2",
	    gridLines: {
		    offsetGridLines: true
		},
    	offset: true
   	});
}

function drawRankChartBase(title, data, params, gubun) {

	drawNewChartCommon(title, data, params, 'line', '변동율(%)', CHART_RANK_TYPE[gubun], drawRankLegend);

	gMainChart.options.tooltips.callbacks = {
       	beforeBody: getTooltip4RankChart
	};

	setPageNavigator(params, data);

	const priceGubun = PriceGubun.getPriceGubun(params);
	let price_name = priceGubun.price_name;
	let rate_name = priceGubun.rate_name;
	let yLabel = priceGubun.title;
	let bprice_name = priceGubun.before_price_name;

	setXAxeForDualBar();
	addRightYAxe(yLabel, 'B');

	makeChartDataset('line', title, 'A', data[rate_name]);
	makeChartDataset('bar', params['base_ym'] + '의 ' + yLabel, 'B', data[price_name], null,
		{
			backgroundColor : 'rgba(0, 255, 0, 0.5)',
			barPercentage: 0.9 
		});
	makeChartDataset('bar', params['years'] + '년 전의' + yLabel, 'B', data[bprice_name], null, 
		{
			backgroundColor : 'rgba(255, 0, 0, 1)', 
			xAxisID: 'X2', 
			barPercentage: 0.5 
		});

	$('#chartLegend').html(gMainChart.generateLegend()); 

	gMainChart.update();
	
}

function drawRankChart(gubun, params) {
	
	let map = document.getElementById("map");
	if (map)
		map.style.display = 'none';

	params['orderby'] = PriceGubun.getPriceGubun(params).getOrderByArr()[0].var_name;
	params['page'] = 1;
	let url = getRankChartURL(gubun, params);
	let title = makeTitle4RankChart(gubun, params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);

		drawRankChartBase(title, data, params, gubun);

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

function getCookie(name) { //가져올 쿠키의 이름을 파라미터 값으로 받고

	var nameOfCookie = name + "="; //쿠키는 "쿠키=값" 형태로 가지고 있어서 뒤에 있는 값을 가져오기 위해 = 포함

	var x = 0;

	while (x <= document.cookie.length) {  //현재 세션에 가지고 있는 쿠키의 총 길이를 가지고 반복

		var y = (x + nameOfCookie.length); //substring으로 찾아낼 쿠키의 이름 길이 저장

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
	var expire = new Date();
	expire.setDate(expire.getDate() + cDay);
	cookies = cName + '=' + escape(cValue) + '; path=/ '; // 한글 깨짐을 막기위해 escape(cValue)를 합니다.
	if (typeof cDay != 'undefined') 
		cookies += ';expires=' + expire.toGMTString() + ';';
	document.cookie = cookies;
}

function makeScatterDataset(data, label, prefix, priceGubun, bgColor, rotation = 0) {

	let scatter_data = []
	for (var i = 0; i < data['labels'].length; i++) {
		scatter_data.push({ x: i*4+1, y: data[prefix + priceGubun.price_name][i] });
	}

	makeChartDataset('scatter', label, 'B', scatter_data, 'red', 
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

	drawNewChartCommon(title, data, params, 'bar', priceGubun.title, CHART_TYPE.COMPARE);

	addRightYAxe("거래건수", 'B');

	makeChartDataset('bar', '1년전', 'A', data['before_1y_'+priceGubun.price_name], 'red', {'backgroundColor':'red'});
	makeScatterDataset(data, '1년전 최고가', 'before_1y_', priceGubun, 'red');
	makeChartDataset('line', '1년전 거래량', 'B', data['before_1y_cnt'], 'red', {'borderColor':'red'});

	makeChartDataset('bar', '1달전', 'A', data['before_1m_'+priceGubun.price_name], 'red', {'backgroundColor':'green'});
	makeScatterDataset(data, '1달전 최고가', 'before_1m_', priceGubun, 'green');
	makeChartDataset('line', '1달전 거래량', 'B', data['before_1m_cnt'], 'red', {'borderColor':'green'});

	makeChartDataset('bar', params['to_ym'], 'A', data['cur_'+priceGubun.price_name], 'red', {'backgroundColor':'black'});
	makeScatterDataset(data, params['to_ym']+' 최고가', 'cur_', priceGubun, 'black');
	makeChartDataset('line', params['to_ym']+' 거래량', 'B', data['cur_cnt'], 'red', {'borderColor':'black'});

	gMainChart.update();

}


function drawSaleCompare(params, title) {
	
	var url = getChartURL('getCompareData', params);

	title = makeChartConditionTitle(title, params);

	showMessage();
	$.getJSON(url, function(jsonData){
		data = JSON.parse(jsonData);
		drawCompareChart(title, data, params);
		closeMessage();
	});

}

