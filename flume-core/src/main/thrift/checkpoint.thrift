/*
 * checkpoint.thrift
 */

namespace java com.nexr.cp.thrift

service CheckPointService {
	bool checkTagId(1:string tag)
}