/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package filters

import (
	"net/http"
)

// WithCacheControl sets the Cache-Control header to "no-cache, private" because all servers are protected by authn/authz.
// see https://developers.google.com/web/fundamentals/performance/optimizing-content-efficiency/http-caching#defining_optimal_cache-control_policy
// 加上cache control，只允许用户自己的浏览器缓存这个响应，而且每次使用前都要重新验证；同时不允许 CDN 或共享代理缓存这个响应。因为每个接口都有权限控制，权限常常会变。
// ✅ Cache-Control: no-cache 的真实含义是：
// 客户端（浏览器）或中间代理（如 CDN）可以缓存响应内容，但是不能在下次请求时直接使用这个缓存内容，必须先向服务器确认是否仍然有效。
//
// 🔁 过程举例：
// 假设用户第一次访问某个页面 /profile，服务器响应中带有：
// Cache-Control: no-cache
// ETag: "abc123"
// 浏览器会把这个响应缓存下来，包括 ETag 字段。
//
// 第二次请求 /profile 时，浏览器会这样做：
//
// 发送请求时带上这个头：
// If-None-Match: "abc123"
// 服务器检查这个 ETag 是否还有效：
//
// ✅ 如果还有效，服务器返回：
// HTTP/1.1 304 Not Modified
// 浏览器就会使用本地缓存的响应体。
//
// ❌ 如果 ETag 不再匹配，服务器返回新的响应（200 OK + 新内容），并更新本地缓存。
func WithCacheControl(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Set the cache-control header if it is not already set
		if _, ok := w.Header()["Cache-Control"]; !ok {
			w.Header().Set("Cache-Control", "no-cache, private")
		}
		handler.ServeHTTP(w, req)
	})
}
