var fs				= require('fs'),
	util			= require('util'),
	async			= require('async'),
	AWS				= require('aws-sdk'),
	EventEmitter	= require('events').EventEmitter;

function _extend(target, source) {
	if (source && typeof source === 'object') {
		Object.getOwnPropertyNames(source).forEach(function (k) {
			var d = Object.getOwnPropertyDescriptor(source, k) || {value: source[k]};

			if (d.get) {
				target.__defineGetter__(k, d.get);

				if (d.set) {
					target.__defineSetter__(k, d.set);
				}
			} else if (target !== d.value) {
				target[k] = d.value;
			}
		});
	}

	return target;
}

function configure(data) {
	AWS.config.update(data);
	return module.exports;
}

function Multipart(data) {
	var _this = this;

	data = _extend({
		bucket: '',
		objectName: '',
		stream: '',
		file: '',
		headers: {},
		concurrency: 4,
		noDisk: true,
		maxPartRetries: 2,
		maxTotalRetries: 6,
		minPartSize: 5 * 1024 * 1024,
		maxPartSize: Infinity,
		maxTotalSize: Infinity
	}, data);

	if (!data.objectName) {
		throw new Error('MPU: `objectName` must be defined');
	}

	if (!data.stream && !data.file) {
		throw new Error('MPU: `stream` or `file` must be passed');
	}

	if (data.stream && data.file) {
		throw new Error('MPU: Both `stream` and `file` cannot be passed together');
	}

	if (data.minPartSize < 5 * 1024 * 1024) {
		throw new Error('MPU: `minPartSize` must be at leasst 5MB per Amazon AWS S3 Guidelines');
	}

	if (data.minPartSize > data.maxSize) {
		throw new Error('MPU: `maxPartSize` must be greater than `minPartSize`');
	}

	this.numParts		= 0;
	this.totalWritten	= 0;
	this.totalSize		= 0;
	this.parts			= [];
	this.aborted		= false;
	this.streamEnded	= false;
	this.buffer			= false;
	this.config			= data;
	this.S3				= new AWS.S3();
	this.queue			= async.queue(this._createPart.bind(this), data.concurrency);

	if (data.stream) {
		this.pending = [];
		this._handleStream();
	}

	this._getUploadId();

	this.on('uploadId', function _onUploadId(uploadId) {
		if (_this.config.file) {
			_this._processFile();
		} else if (_this.config.stream) {
			_this._processStream();
		}
	});
}

util.inherits(Multipart, EventEmitter);

Multipart.prototype._getUploadId = function () {
	var params,
		_this = this;

	params = _extend({
		ACL: 'private',
	}, this.config.headers);

	params.Bucket	= this.config.bucket;
	params.Key		= this.config.objectName;

	this.S3.createMultipartUpload(params, function _createMultipartUpload(err, data) {
		if (err) {
			_this.emit('error', err);
			return;
		}

		_this.uploadId = data.UploadId;

		_this.emit('uploadId', data.UploadId);
	});
};

Multipart.prototype._processFile = function () {
	var _this = this;

	fs.exists(this.config.file, function _profileFileExists(exists) {
		if (!exists) {
			_this.emit('error', new Error('File does not exist: ' + _this.config.file));
			_this.abort();
			return;
		}

		_this.config.stream = fs.createReadStream(_this.config.file);
		_this._handleStream();
	})
};

Multipart.prototype._processStream = function () {
	if (this.pending) {
		this._handlePendingQueue();
	}
};

Multipart.prototype._handleStream = function () {
	var _this = this;

	this.config.stream.on('data', function _handleStreamOnData(data) {
		_this.totalSize += data.length;

		if (_this.totalSize > _this.maxTotalSize) {
			_this.emit('uploadSizeError', _this.totalSize);
			_this.abort();
			return;
		}

		if (_this.buffer) {
			_this.buffer = Buffer.concat([_this.buffer, data], _this.buffer.length + data.length);

			if (_this.buffer.length >= _this.config.minPartSize) {
				_this._pushPart(_this.buffer);
				_this.buffer = false;
			}
		} else if (data.length < _this.config.minPartSize) {
			_this.buffer = data;
		} else if (data.length > _this.config.maxPartSize) {
			// TODO: Handle this situation
			_this._pushPart(data);
		} else {
			_this._pushPart(data);
		}
	});

	this.config.stream.on('error', function _handleStreamOnError(err) {
		_this.emit('error', err);
		_this.abort();
	});

	this.config.stream.on('end', function _handleStreamOnEnd() {
		if (_this.buffer) {
			_this._pushPart(_this.buffer);
			_this.buffer = false;
		}

		_this.streamEnded	= true;
		_this.queue.drain	= _this._handleComplete.bind(_this);
	});
};

Multipart.prototype._pushPart = function (data, route) {
	(this.pending || this.queue).push({partId: ++this.numParts, data: data});
};

Multipart.prototype._handleComplete = function () {
	var params,
		_this = this;

	if (this.aborted) {
		return;
	}

	this.emit('completing', this.uploadId);

	params = {
		Bucket: this.config.bucket,
		Key: this.config.objectName,
		UploadId: this.uploadId,
		MultipartUpload: {
			Parts: this.parts.sort(this._handleSort)
		}
	};

	this.S3.completeMultipartUpload(params, function _completeMultipartUpload(err, data) {
		if (err) {
			_this.emit('error', err);
			_this.abort();
			return;
		}

		_this.emit('complete', this.uploadId);
	});
};

Multipart.prototype._handleSort = function (a, b) {
	return a.PartNumber < b.PartNumber ? -1 : 1;
};

Multipart.prototype._handlePendingQueue = function () {
	while (this.pending.length) {
		this.queue.push(this.pending.shift());
	}

	this.pending = false;
};

Multipart.prototype._createPart = function (part, callback, retry) {
	var params, req,
		_this = this;

	retry = retry || 0;

	if (this.aborted || !part.data.length) {
		callback(this.aborted);
		return;
	}

	params = {
		Bucket: this.config.bucket,
		Key: this.config.objectName,
		PartNumber: part.partId,
		UploadId: this.uploadId,
		Body: part.data,
		ContentLength: part.data.length
	};

	this.emit('uploading', part.partId);

	req = this.S3.uploadPart(params, function _uploadPart(err, data) {
		var info;

		if (_this.aborted) {
			callback(true);
			return;
		}

		if (err) {
			if (retry < _this.config.maxPartRetries) {
				_this.emit('partRetried', part.partId);
				_this._createPart(part, callback, retry + 1);
				return;
			}

			callback(true);
			_this.emit('error', err);
			_this.abort();
			return;
		}

		_this.parts.push({
			ETag: data.ETag,
			PartNumber: part.partId
		});

		_this.totalWritten += part.data.length;

		info = {
			part: part.partId,
			partSize: part.data.length,
			totalWritten: _this.totalWritten,
			totalSize: _this.totalSize,
			totalPercent: _this.streamEnded ? _this.totalWritten / _this.totalSize * 100 : 0
		};

		_this.emit('uploaded', part.partId);
		_this.emit('progress', info);
		callback();
	});

	req.on('httpUploadProgress', function _onHttpUploadProgress(data) {
		_this.emit('partProgress', data);
	});
};

Multipart.prototype.abort = function () {
	var params,
		_this = this;

	if (this.aborted) {
		return;
	}

	params = {
		Bucket: this.config.bucket,
		Key: this.config.objectName,
		UploadId: this.uploadId
	};

	this.aborted	= true;
	this.pending	= false;
	this.buffer		= false;
	this.parts		= undefined;
	this.config		= undefined;

	this.queue.kill();

	_this.emit('aborting', _this.uploadId);

	this.S3.abortMultipartUpload(params, function _abortMultipartUpload(err, data) {
		if (err) {
			_this.emit('error', err);
			return;
		}

		_this.S3 = undefined;
		_this.emit('aborted', _this.uploadId);
	});
};

Multipart.prototype.pause = function () {
	this.queue.pause();
};

Multipart.prototype.resume = function () {
	this.queue.resume();
};

module.exports				= Multipart;
module.exports.configure	= configure;