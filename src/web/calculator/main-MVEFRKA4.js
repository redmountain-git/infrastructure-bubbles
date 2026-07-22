var rh=Object.defineProperty,Kw=Object.defineProperties,Qw=Object.getOwnPropertyDescriptor,Zw=Object.getOwnPropertyDescriptors;var th=Object.getOwnPropertySymbols;var Yw=Object.prototype.hasOwnProperty,Jw=Object.prototype.propertyIsEnumerable;var oh=t=>{throw TypeError(t)};var nh=(t,e,n)=>e in t?rh(t,e,{enumerable:!0,configurable:!0,writable:!0,value:n}):t[e]=n,m=(t,e)=>{for(var n in e||={})Yw.call(e,n)&&nh(t,n,e[n]);if(th)for(var n of th(e))Jw.call(e,n)&&nh(t,n,e[n]);return t},T=(t,e)=>Kw(t,Zw(e));var f=(t,e,n)=>()=>{if(n)throw n[0];try{return t&&(e=t(t=0)),e}catch(r){throw n=[r],r}};var Xw=(t,e)=>()=>{try{return e||t((e={exports:{}}).exports,e),e.exports}catch(n){throw e=0,n}};var v=(t,e,n,r)=>{for(var o=r>1?void 0:r?Qw(e,n):e,i=t.length-1,s;i>=0;i--)(s=t[i])&&(o=(r?s(e,n,o):s(o))||o);return r&&o&&rh(e,n,o),o};var ih=(t,e,n)=>e.has(t)||oh("Cannot "+n);var M=(t,e,n)=>(ih(t,e,"read from private field"),n?n.call(t):e.get(t)),N=(t,e,n)=>e.has(t)?oh("Cannot add the same private member more than once"):e instanceof WeakSet?e.add(t):e.set(t,n),R=(t,e,n,r)=>(ih(t,e,"write to private field"),r?r.call(t,n):e.set(t,n),n);function B(t){let e=Oe;return Oe=t,e}function Ns(){return Oe}function Or(t){if(xs)throw new Error("");if(Oe===null)return;Oe.consumerOnSignalRead(t);let e=Oe.producersTail;if(e!==void 0&&e.producer===t)return;let n,r=Oe.recomputing;if(r&&(n=e!==void 0?e.nextProducer:Oe.producers,n!==void 0&&n.producer===t)){Oe.producersTail=n,n.lastReadVersion=t.version;return}let o=t.consumersTail;if(o!==void 0&&o.consumer===Oe&&(!r||nI(o,Oe)))return;let i=Lr(Oe),s={producer:t,consumer:Oe,nextProducer:n,prevConsumer:o,lastReadVersion:t.version,nextConsumer:void 0};Oe.producersTail=s,e!==void 0?e.nextProducer=s:Oe.producers=s,i&&lh(t,s)}function sh(){Tl++}function Rs(t){if(!(Lr(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Tl)){if(!t.producerMustRecompute(t)&&!ks(t)){Ms(t);return}t.producerRecomputeValue(t),Ms(t)}}function Ml(t){if(t.consumers===void 0)return;let e=xs;xs=!0;try{for(let n=t.consumers;n!==void 0;n=n.nextConsumer){let r=n.consumer;r.dirty||tI(r)}}finally{xs=e}}function Nl(){return Oe?.consumerAllowSignalWrites!==!1}function tI(t){t.dirty=!0,Ml(t),t.consumerMarkedDirty?.(t)}function Ms(t){t.dirty=!1,t.lastCleanEpoch=Tl}function Pr(t){return t&&ah(t),B(t)}function ah(t){t.producersTail=void 0,t.recomputing=!0}function Ho(t,e){B(e),t&&ch(t)}function ch(t){t.recomputing=!1;let e=t.producersTail,n=e!==void 0?e.nextProducer:t.producers;if(n!==void 0){if(Lr(t))do n=Rl(n);while(n!==void 0);e!==void 0?e.nextProducer=void 0:t.producers=void 0}}function ks(t){for(let e=t.producers;e!==void 0;e=e.nextProducer){let n=e.producer,r=e.lastReadVersion;if(r!==n.version||(Rs(n),r!==n.version))return!0}return!1}function Bo(t){if(Lr(t)){let e=t.producers;for(;e!==void 0;)e=Rl(e)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function lh(t,e){let n=t.consumersTail,r=Lr(t);if(n!==void 0?(e.nextConsumer=n.nextConsumer,n.nextConsumer=e):(e.nextConsumer=void 0,t.consumers=e),e.prevConsumer=n,t.consumersTail=e,!r)for(let o=t.producers;o!==void 0;o=o.nextProducer)lh(o.producer,o)}function Rl(t){let e=t.producer,n=t.nextProducer,r=t.nextConsumer,o=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,r!==void 0?r.prevConsumer=o:e.consumersTail=o,o!==void 0)o.nextConsumer=r;else if(e.consumers=r,!Lr(e)){let i=e.producers;for(;i!==void 0;)i=Rl(i)}return n}function Lr(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function As(t){eI?.(t)}function nI(t,e){let n=e.producersTail;if(n!==void 0){let r=e.producers;do{if(r===t)return!0;if(r===n)break;r=r.nextProducer}while(r!==void 0)}return!1}function Os(t,e){return Object.is(t,e)}function Ps(t,e){let n=Object.create(rI);n.computation=t,e!==void 0&&(n.equal=e);let r=()=>{if(Rs(n),Or(n),n.value===Uo)throw n.error;return n.value};return r[qe]=n,As(n),r}function oI(){throw new Error}function dh(t){uh(t)}function kl(t){uh=t}function Al(t,e){let n=Object.create(Ls);n.value=t,e!==void 0&&(n.equal=e);let r=()=>fh(n);return r[qe]=n,As(n),[r,s=>$r(n,s),s=>Ol(n,s)]}function fh(t){return Or(t),t.value}function $r(t,e){Nl()||dh(t),t.equal(t.value,e)||(t.value=e,sI(t))}function Ol(t,e){Nl()||dh(t),$r(t,e(t.value))}function sI(t){t.version++,sh(),Ml(t),iI?.(t)}var Oe,xs,Tl,eI,qe,Ar,Ds,Ts,Uo,rI,uh,iI,Ls,Hn=f(()=>{"use strict";Oe=null,xs=!1,Tl=1,eI=null,qe=Symbol("SIGNAL");Ar={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};Ds=Symbol("UNSET"),Ts=Symbol("COMPUTING"),Uo=Symbol("ERRORED"),rI=T(m({},Ar),{value:Ds,dirty:!0,error:null,equal:Os,kind:"computed",producerMustRecompute(t){return t.value===Ds||t.value===Ts},producerRecomputeValue(t){if(t.value===Ts)throw new Error("");let e=t.value;t.value=Ts;let n=Pr(t),r,o=!1;try{r=t.computation(),B(null),o=e!==Ds&&e!==Uo&&r!==Uo&&t.equal(e,r)}catch(i){r=Uo,t.error=i}finally{Ho(t,n)}if(o){t.value=e;return}t.value=r,t.version++}});uh=oI;iI=null;Ls=T(m({},Ar),{equal:Os,value:void 0,kind:"signal"})});function $(t){return typeof t=="function"}var ve=f(()=>{"use strict"});function Vr(t){let n=t(r=>{Error.call(r),r.stack=new Error().stack});return n.prototype=Object.create(Error.prototype),n.prototype.constructor=n,n}var $s=f(()=>{"use strict"});var Vs,ph=f(()=>{"use strict";$s();Vs=Vr(t=>function(n){t(this),this.message=n?`${n.length} errors occurred during unsubscription:
${n.map((r,o)=>`${o+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=n})});function zo(t,e){if(t){let n=t.indexOf(e);0<=n&&t.splice(n,1)}}var Pl=f(()=>{"use strict"});function Fs(t){return t instanceof De||t&&"closed"in t&&$(t.remove)&&$(t.add)&&$(t.unsubscribe)}function hh(t){$(t)?t():t.unsubscribe()}var De,Ll,qo=f(()=>{"use strict";ve();ph();Pl();De=class t{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:n}=this;if(n)if(this._parentage=null,Array.isArray(n))for(let i of n)i.remove(this);else n.remove(this);let{initialTeardown:r}=this;if($(r))try{r()}catch(i){e=i instanceof Vs?i.errors:[i]}let{_finalizers:o}=this;if(o){this._finalizers=null;for(let i of o)try{hh(i)}catch(s){e=e??[],s instanceof Vs?e=[...e,...s.errors]:e.push(s)}}if(e)throw new Vs(e)}}add(e){var n;if(e&&e!==this)if(this.closed)hh(e);else{if(e instanceof t){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(n=this._finalizers)!==null&&n!==void 0?n:[]).push(e)}}_hasParent(e){let{_parentage:n}=this;return n===e||Array.isArray(n)&&n.includes(e)}_addParent(e){let{_parentage:n}=this;this._parentage=Array.isArray(n)?(n.push(e),n):n?[n,e]:e}_removeParent(e){let{_parentage:n}=this;n===e?this._parentage=null:Array.isArray(n)&&zo(n,e)}remove(e){let{_finalizers:n}=this;n&&zo(n,e),e instanceof t&&e._removeParent(this)}};De.EMPTY=(()=>{let t=new De;return t.closed=!0,t})();Ll=De.EMPTY});var It,Wo=f(()=>{"use strict";It={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1}});var Fr,$l=f(()=>{"use strict";Fr={setTimeout(t,e,...n){let{delegate:r}=Fr;return r?.setTimeout?r.setTimeout(t,e,...n):setTimeout(t,e,...n)},clearTimeout(t){let{delegate:e}=Fr;return(e?.clearTimeout||clearTimeout)(t)},delegate:void 0}});function js(t){Fr.setTimeout(()=>{let{onUnhandledError:e}=It;if(e)e(t);else throw t})}var Vl=f(()=>{"use strict";Wo();$l()});function Bn(){}var Us=f(()=>{"use strict"});function gh(t){return Fl("E",void 0,t)}function vh(t){return Fl("N",t,void 0)}function Fl(t,e,n){return{kind:t,value:e,error:n}}var mh,yh=f(()=>{"use strict";mh=Fl("C",void 0,void 0)});function jr(t){if(It.useDeprecatedSynchronousErrorHandling){let e=!zn;if(e&&(zn={errorThrown:!1,error:null}),t(),e){let{errorThrown:n,error:r}=zn;if(zn=null,n)throw r}}else t()}function bh(t){It.useDeprecatedSynchronousErrorHandling&&zn&&(zn.errorThrown=!0,zn.error=t)}var zn,Hs=f(()=>{"use strict";Wo();zn=null});function jl(t,e){return aI.call(t,e)}function Bs(t){It.useDeprecatedSynchronousErrorHandling?bh(t):js(t)}function cI(t){throw t}function Ul(t,e){let{onStoppedNotification:n}=It;n&&Fr.setTimeout(()=>n(t,e))}var qn,aI,Hl,Ur,lI,Bl=f(()=>{"use strict";ve();qo();Wo();Vl();Us();yh();$l();Hs();qn=class extends De{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,Fs(e)&&e.add(this)):this.destination=lI}static create(e,n,r){return new Ur(e,n,r)}next(e){this.isStopped?Ul(vh(e),this):this._next(e)}error(e){this.isStopped?Ul(gh(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Ul(mh,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},aI=Function.prototype.bind;Hl=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:n}=this;if(n.next)try{n.next(e)}catch(r){Bs(r)}}error(e){let{partialObserver:n}=this;if(n.error)try{n.error(e)}catch(r){Bs(r)}else Bs(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(n){Bs(n)}}},Ur=class extends qn{constructor(e,n,r){super();let o;if($(e)||!e)o={next:e??void 0,error:n??void 0,complete:r??void 0};else{let i;this&&It.useDeprecatedNextContext?(i=Object.create(e),i.unsubscribe=()=>this.unsubscribe(),o={next:e.next&&jl(e.next,i),error:e.error&&jl(e.error,i),complete:e.complete&&jl(e.complete,i)}):o=e}this.destination=new Hl(o)}};lI={closed:!0,next:Bn,error:cI,complete:Bn}});var Hr,zs=f(()=>{"use strict";Hr=typeof Symbol=="function"&&Symbol.observable||"@@observable"});function St(t){return t}var Br=f(()=>{"use strict"});function zl(...t){return ql(t)}function ql(t){return t.length===0?St:t.length===1?t[0]:function(n){return t.reduce((r,o)=>o(r),n)}}var Wl=f(()=>{"use strict";Br()});function _h(t){var e;return(e=t??It.Promise)!==null&&e!==void 0?e:Promise}function uI(t){return t&&$(t.next)&&$(t.error)&&$(t.complete)}function dI(t){return t&&t instanceof qn||uI(t)&&Fs(t)}var z,ot=f(()=>{"use strict";Bl();qo();zs();Wl();Wo();ve();Hs();z=(()=>{class t{constructor(n){n&&(this._subscribe=n)}lift(n){let r=new t;return r.source=this,r.operator=n,r}subscribe(n,r,o){let i=dI(n)?n:new Ur(n,r,o);return jr(()=>{let{operator:s,source:a}=this;i.add(s?s.call(i,a):a?this._subscribe(i):this._trySubscribe(i))}),i}_trySubscribe(n){try{return this._subscribe(n)}catch(r){n.error(r)}}forEach(n,r){return r=_h(r),new r((o,i)=>{let s=new Ur({next:a=>{try{n(a)}catch(c){i(c),s.unsubscribe()}},error:i,complete:o});this.subscribe(s)})}_subscribe(n){var r;return(r=this.source)===null||r===void 0?void 0:r.subscribe(n)}[Hr](){return this}pipe(...n){return ql(n)(this)}toPromise(n){return n=_h(n),new n((r,o)=>{let i;this.subscribe(s=>i=s,s=>o(s),()=>r(i))})}}return t.create=e=>new t(e),t})()});function fI(t){return $(t?.lift)}function X(t){return e=>{if(fI(e))return e.lift(function(n){try{return t(n,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}var Pe=f(()=>{"use strict";ve()});function oe(t,e,n,r,o){return new Gl(t,e,n,r,o)}var Gl,Ye=f(()=>{"use strict";Bl();Gl=class extends qn{constructor(e,n,r,o,i,s){super(e),this.onFinalize=i,this.shouldUnsubscribe=s,this._next=n?function(a){try{n(a)}catch(c){e.error(c)}}:super._next,this._error=o?function(a){try{o(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:n}=this;super.unsubscribe(),!n&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}}});var Eh,wh=f(()=>{"use strict";$s();Eh=Vr(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"})});var me,qs,Kl=f(()=>{"use strict";ot();qo();wh();Pl();Hs();me=(()=>{class t extends z{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let r=new qs(this,this);return r.operator=n,r}_throwIfClosed(){if(this.closed)throw new Eh}next(n){jr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let r of this.currentObservers)r.next(n)}})}error(n){jr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:r}=this;for(;r.length;)r.shift().error(n)}})}complete(){jr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:r,isStopped:o,observers:i}=this;return r||o?Ll:(this.currentObservers=null,i.push(n),new De(()=>{this.currentObservers=null,zo(i,n)}))}_checkFinalizedStatuses(n){let{hasError:r,thrownError:o,isStopped:i}=this;r?n.error(o):i&&n.complete()}asObservable(){let n=new z;return n.source=this,n}}return t.create=(e,n)=>new qs(e,n),t})(),qs=class extends me{constructor(e,n){super(),this.destination=e,this.source=n}next(e){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.next)===null||r===void 0||r.call(n,e)}error(e){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.error)===null||r===void 0||r.call(n,e)}complete(){var e,n;(n=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||n===void 0||n.call(e)}_subscribe(e){var n,r;return(r=(n=this.source)===null||n===void 0?void 0:n.subscribe(e))!==null&&r!==void 0?r:Ll}}});var Te,Ih=f(()=>{"use strict";Kl();Te=class extends me{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let n=super._subscribe(e);return!n.closed&&e.next(this._value),n}getValue(){let{hasError:e,thrownError:n,_value:r}=this;if(e)throw n;return this._throwIfClosed(),r}next(e){super.next(this._value=e)}}});var Me,Ws=f(()=>{"use strict";ot();Me=new z(t=>t.complete())});function Sh(t){return t&&$(t.schedule)}var Ch=f(()=>{"use strict";ve()});function xh(t){return t[t.length-1]}function Dh(t){return $(xh(t))?t.pop():void 0}function vn(t){return Sh(xh(t))?t.pop():void 0}var Go=f(()=>{"use strict";ve();Ch()});function Mh(t,e,n,r){function o(i){return i instanceof n?i:new n(function(s){s(i)})}return new(n||(n=Promise))(function(i,s){function a(u){try{l(r.next(u))}catch(d){s(d)}}function c(u){try{l(r.throw(u))}catch(d){s(d)}}function l(u){u.done?i(u.value):o(u.value).then(a,c)}l((r=r.apply(t,e||[])).next())})}function Th(t){var e=typeof Symbol=="function"&&Symbol.iterator,n=e&&t[e],r=0;if(n)return n.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&r>=t.length&&(t=void 0),{value:t&&t[r++],done:!t}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Wn(t){return this instanceof Wn?(this.v=t,this):new Wn(t)}function Nh(t,e,n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=n.apply(t,e||[]),o,i=[];return o=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),o[Symbol.asyncIterator]=function(){return this},o;function s(p){return function(g){return Promise.resolve(g).then(p,d)}}function a(p,g){r[p]&&(o[p]=function(E){return new Promise(function(b,D){i.push([p,E,b,D])>1||c(p,E)})},g&&(o[p]=g(o[p])))}function c(p,g){try{l(r[p](g))}catch(E){h(i[0][3],E)}}function l(p){p.value instanceof Wn?Promise.resolve(p.value.v).then(u,d):h(i[0][2],p)}function u(p){c("next",p)}function d(p){c("throw",p)}function h(p,g){p(g),i.shift(),i.length&&c(i[0][0],i[0][1])}}function Rh(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=t[Symbol.asyncIterator],n;return e?e.call(t):(t=typeof Th=="function"?Th(t):t[Symbol.iterator](),n={},r("next"),r("throw"),r("return"),n[Symbol.asyncIterator]=function(){return this},n);function r(i){n[i]=t[i]&&function(s){return new Promise(function(a,c){s=t[i](s),o(a,c,s.done,s.value)})}}function o(i,s,a,c){Promise.resolve(c).then(function(l){i({value:l,done:a})},s)}}var Ql=f(()=>{"use strict"});var Gs,Zl=f(()=>{"use strict";Gs=t=>t&&typeof t.length=="number"&&typeof t!="function"});function Ks(t){return $(t?.then)}var Yl=f(()=>{"use strict";ve()});function Qs(t){return $(t[Hr])}var Jl=f(()=>{"use strict";zs();ve()});function Zs(t){return Symbol.asyncIterator&&$(t?.[Symbol.asyncIterator])}var Xl=f(()=>{"use strict";ve()});function Ys(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}var eu=f(()=>{"use strict"});function pI(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Js,tu=f(()=>{"use strict";Js=pI()});function Xs(t){return $(t?.[Js])}var nu=f(()=>{"use strict";tu();ve()});function ea(t){return Nh(this,arguments,function*(){let n=t.getReader();try{for(;;){let{value:r,done:o}=yield Wn(n.read());if(o)return yield Wn(void 0);yield yield Wn(r)}}finally{n.releaseLock()}})}function ta(t){return $(t?.getReader)}var na=f(()=>{"use strict";Ql();ve()});function Ie(t){if(t instanceof z)return t;if(t!=null){if(Qs(t))return hI(t);if(Gs(t))return mI(t);if(Ks(t))return gI(t);if(Zs(t))return kh(t);if(Xs(t))return vI(t);if(ta(t))return yI(t)}throw Ys(t)}function hI(t){return new z(e=>{let n=t[Hr]();if($(n.subscribe))return n.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function mI(t){return new z(e=>{for(let n=0;n<t.length&&!e.closed;n++)e.next(t[n]);e.complete()})}function gI(t){return new z(e=>{t.then(n=>{e.closed||(e.next(n),e.complete())},n=>e.error(n)).then(null,js)})}function vI(t){return new z(e=>{for(let n of t)if(e.next(n),e.closed)return;e.complete()})}function kh(t){return new z(e=>{bI(t,e).catch(n=>e.error(n))})}function yI(t){return kh(ea(t))}function bI(t,e){var n,r,o,i;return Mh(this,void 0,void 0,function*(){try{for(n=Rh(t);r=yield n.next(),!r.done;){let s=r.value;if(e.next(s),e.closed)return}}catch(s){o={error:s}}finally{try{r&&!r.done&&(i=n.return)&&(yield i.call(n))}finally{if(o)throw o.error}}e.complete()})}var Vt=f(()=>{"use strict";Ql();Zl();Yl();ot();Jl();Xl();eu();nu();na();ve();Vl();zs()});function Je(t,e,n,r=0,o=!1){let i=e.schedule(function(){n(),o?t.add(this.schedule(null,r)):this.unsubscribe()},r);if(t.add(i),!o)return i}var zr=f(()=>{"use strict"});function ra(t,e=0){return X((n,r)=>{n.subscribe(oe(r,o=>Je(r,t,()=>r.next(o),e),()=>Je(r,t,()=>r.complete(),e),o=>Je(r,t,()=>r.error(o),e)))})}var ru=f(()=>{"use strict";zr();Pe();Ye()});function oa(t,e=0){return X((n,r)=>{r.add(t.schedule(()=>n.subscribe(r),e))})}var ou=f(()=>{"use strict";Pe()});function Ah(t,e){return Ie(t).pipe(oa(e),ra(e))}var Oh=f(()=>{"use strict";Vt();ru();ou()});function Ph(t,e){return Ie(t).pipe(oa(e),ra(e))}var Lh=f(()=>{"use strict";Vt();ru();ou()});function $h(t,e){return new z(n=>{let r=0;return e.schedule(function(){r===t.length?n.complete():(n.next(t[r++]),n.closed||this.schedule())})})}var Vh=f(()=>{"use strict";ot()});function Fh(t,e){return new z(n=>{let r;return Je(n,e,()=>{r=t[Js](),Je(n,e,()=>{let o,i;try{({value:o,done:i}=r.next())}catch(s){n.error(s);return}i?n.complete():n.next(o)},0,!0)}),()=>$(r?.return)&&r.return()})}var jh=f(()=>{"use strict";ot();tu();ve();zr()});function ia(t,e){if(!t)throw new Error("Iterable cannot be null");return new z(n=>{Je(n,e,()=>{let r=t[Symbol.asyncIterator]();Je(n,e,()=>{r.next().then(o=>{o.done?n.complete():n.next(o.value)})},0,!0)})})}var iu=f(()=>{"use strict";ot();zr()});function Uh(t,e){return ia(ea(t),e)}var Hh=f(()=>{"use strict";iu();na()});function Bh(t,e){if(t!=null){if(Qs(t))return Ah(t,e);if(Gs(t))return $h(t,e);if(Ks(t))return Ph(t,e);if(Zs(t))return ia(t,e);if(Xs(t))return Fh(t,e);if(ta(t))return Uh(t,e)}throw Ys(t)}var zh=f(()=>{"use strict";Oh();Lh();Vh();jh();iu();Jl();Yl();Zl();nu();Xl();eu();na();Hh()});function ge(t,e){return e?Bh(t,e):Ie(t)}var Ko=f(()=>{"use strict";zh();Vt()});function q(...t){let e=vn(t);return ge(t,e)}var qh=f(()=>{"use strict";Go();Ko()});function su(t,e){let n=$(t)?t:()=>t,r=o=>o.error(n());return new z(e?o=>e.schedule(r,0,o):r)}var Wh=f(()=>{"use strict";ot();ve()});function sa(t){return!!t&&(t instanceof z||$(t.lift)&&$(t.subscribe))}var Gh=f(()=>{"use strict";ot();ve()});var Gn,aa=f(()=>{"use strict";$s();Gn=Vr(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"})});function ne(t,e){return X((n,r)=>{let o=0;n.subscribe(oe(r,i=>{r.next(t.call(e,i,o++))}))})}var ca=f(()=>{"use strict";Pe();Ye()});function EI(t,e){return _I(e)?t(...e):t(e)}function Kh(t){return ne(e=>EI(t,e))}var _I,Qh=f(()=>{"use strict";ca();({isArray:_I}=Array)});function Zh(t){if(t.length===1){let e=t[0];if(wI(e))return{args:e,keys:null};if(xI(e)){let n=CI(e);return{args:n.map(r=>e[r]),keys:n}}}return{args:t,keys:null}}function xI(t){return t&&typeof t=="object"&&II(t)===SI}var wI,II,SI,CI,Yh=f(()=>{"use strict";({isArray:wI}=Array),{getPrototypeOf:II,prototype:SI,keys:CI}=Object});function Jh(t,e){return t.reduce((n,r,o)=>(n[r]=e[o],n),{})}var Xh=f(()=>{"use strict"});function au(...t){let e=vn(t),n=Dh(t),{args:r,keys:o}=Zh(t);if(r.length===0)return ge([],e);let i=new z(DI(r,e,o?s=>Jh(o,s):St));return n?i.pipe(Kh(n)):i}function DI(t,e,n=St){return r=>{em(e,()=>{let{length:o}=t,i=new Array(o),s=o,a=o;for(let c=0;c<o;c++)em(e,()=>{let l=ge(t[c],e),u=!1;l.subscribe(oe(r,d=>{i[c]=d,u||(u=!0,a--),a||r.next(n(i.slice()))},()=>{--s||r.complete()}))},r)},r)}}function em(t,e,n){t?Je(n,t,e):e()}var tm=f(()=>{"use strict";ot();Yh();Ko();Br();Qh();Go();Xh();Ye();zr()});function nm(t,e,n,r,o,i,s,a){let c=[],l=0,u=0,d=!1,h=()=>{d&&!c.length&&!l&&e.complete()},p=E=>l<r?g(E):c.push(E),g=E=>{i&&e.next(E),l++;let b=!1;Ie(n(E,u++)).subscribe(oe(e,D=>{o?.(D),i?p(D):e.next(D)},()=>{b=!0},void 0,()=>{if(b)try{for(l--;c.length&&l<r;){let D=c.shift();s?Je(e,s,()=>g(D)):g(D)}h()}catch(D){e.error(D)}}))};return t.subscribe(oe(e,p,()=>{d=!0,h()})),()=>{a?.()}}var rm=f(()=>{"use strict";Vt();zr();Ye()});function We(t,e,n=1/0){return $(e)?We((r,o)=>ne((i,s)=>e(r,i,o,s))(Ie(t(r,o))),n):(typeof e=="number"&&(n=e),X((r,o)=>nm(r,o,t,n)))}var la=f(()=>{"use strict";ca();Vt();Pe();rm();ve()});function om(t=1/0){return We(St,t)}var im=f(()=>{"use strict";la();Br()});function sm(){return om(1)}var am=f(()=>{"use strict";im()});function qr(...t){return sm()(ge(t,vn(t)))}var cu=f(()=>{"use strict";am();Go();Ko()});function Qo(t){return new z(e=>{Ie(t()).subscribe(e)})}var cm=f(()=>{"use strict";ot();Vt()});var Kt,lm=f(()=>{"use strict";ot();Us();Kt=new z(Bn)});function pt(t,e){return X((n,r)=>{let o=0;n.subscribe(oe(r,i=>t.call(e,i,o++)&&r.next(i)))})}var lu=f(()=>{"use strict";Pe();Ye()});var um=f(()=>{"use strict"});function Zo(t){return X((e,n)=>{let r=null,o=!1,i;r=e.subscribe(oe(n,void 0,void 0,s=>{i=Ie(t(s,Zo(t)(e))),r?(r.unsubscribe(),r=null,i.subscribe(n)):o=!0})),o&&(r.unsubscribe(),r=null,i.subscribe(n))})}var dm=f(()=>{"use strict";Vt();Ye();Pe()});function ua(t,e){return $(e)?We(t,e,1):We(t,1)}var fm=f(()=>{"use strict";la();ve()});function pm(t){return X((e,n)=>{let r=!1;e.subscribe(oe(n,o=>{r=!0,n.next(o)},()=>{r||n.next(t),n.complete()}))})}var hm=f(()=>{"use strict";Pe();Ye()});function Ge(t){return t<=0?()=>Me:X((e,n)=>{let r=0;e.subscribe(oe(n,o=>{++r<=t&&(n.next(o),t<=r&&n.complete())}))})}var uu=f(()=>{"use strict";Ws();Pe();Ye()});function mm(t=TI){return X((e,n)=>{let r=!1;e.subscribe(oe(n,o=>{r=!0,n.next(o)},()=>r?n.complete():n.error(t())))})}function TI(){return new Gn}var gm=f(()=>{"use strict";aa();Pe();Ye()});function du(t){return X((e,n)=>{try{e.subscribe(n)}finally{n.add(t)}})}var vm=f(()=>{"use strict";Pe()});function Qt(t,e){let n=arguments.length>=2;return r=>r.pipe(t?pt((o,i)=>t(o,i,r)):St,Ge(1),n?pm(e):mm(()=>new Gn))}var ym=f(()=>{"use strict";aa();lu();uu();hm();gm();Br()});function da(t){return t<=0?()=>Me:X((e,n)=>{let r=[];e.subscribe(oe(n,o=>{r.push(o),t<r.length&&r.shift()},()=>{for(let o of r)n.next(o);n.complete()},void 0,()=>{r=null}))})}var bm=f(()=>{"use strict";Ws();Pe();Ye()});function fu(...t){let e=vn(t);return X((n,r)=>{(e?qr(t,n,e):qr(t,n)).subscribe(r)})}var _m=f(()=>{"use strict";cu();Go();Pe()});function Le(t,e){return X((n,r)=>{let o=null,i=0,s=!1,a=()=>s&&!o&&r.complete();n.subscribe(oe(r,c=>{o?.unsubscribe();let l=0,u=i++;Ie(t(c,u)).subscribe(o=oe(r,d=>r.next(e?e(c,d,u,l++):d),()=>{o=null,a()}))},()=>{s=!0,a()}))})}var Em=f(()=>{"use strict";Vt();Pe();Ye()});function Yo(t){return X((e,n)=>{Ie(t).subscribe(oe(n,()=>n.complete(),Bn)),!n.closed&&e.subscribe(n)})}var wm=f(()=>{"use strict";Pe();Ye();Vt();Us()});function ht(t,e,n){let r=$(t)||e||n?{next:t,error:e,complete:n}:t;return r?X((o,i)=>{var s;(s=r.subscribe)===null||s===void 0||s.call(r);let a=!0;o.subscribe(oe(i,c=>{var l;(l=r.next)===null||l===void 0||l.call(r,c),i.next(c)},()=>{var c;a=!1,(c=r.complete)===null||c===void 0||c.call(r),i.complete()},c=>{var l;a=!1,(l=r.error)===null||l===void 0||l.call(r,c),i.error(c)},()=>{var c,l;a&&((c=r.unsubscribe)===null||c===void 0||c.call(r)),(l=r.finalize)===null||l===void 0||l.call(r)}))}):St}var Im=f(()=>{"use strict";ve();Pe();Ye();Br()});var Kn=f(()=>{"use strict";ot();Kl();Ih();qo();Wl();Gh();aa();tm();cu();cm();Ko();qh();Wh();Ws();lm();um()});function fa(){return pu}function Ft(t){let e=pu;return pu=t,e}function Wr(t){return t===Sm||t?.name==="\u0275NotFound"}var pu,Sm,hu=f(()=>{"use strict";Sm=Symbol("NotFound")});function Cm(t){let e=B(null);try{return t()}finally{B(e)}}var xm=f(()=>{"use strict";Hn();});var Dm=f(()=>{"use strict";Hn();});var Tm=f(()=>{"use strict";hu();});function RI(t){return`NG0${Math.abs(t)}`}function Xn(t,e){return`${RI(t)}${e?": "+e:""}`}function ee(t){for(let e in t)if(t[e]===ee)return e;throw Error("")}function ri(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ri).join(", ")}]`;if(t==null)return""+t;let e=t.overriddenName||t.name;if(e)return`${e}`;let n=t.toString();if(n==null)return""+n;let r=n.indexOf(`
`);return r>=0?n.slice(0,r):n}function Nu(t,e){return t?e?`${t} ${e}`:t:e||""}function ya(t){return t.__forward_ref__=ya,t}function Xe(t){return Ru(t)?t():t}function Ru(t){return typeof t=="function"&&t.hasOwnProperty(kI)&&t.__forward_ref__===ya}function A(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function oi(t){return AI(t,ba)}function ku(t){return oi(t)!==null}function AI(t,e){return t.hasOwnProperty(e)&&t[e]||null}function OI(t){let e=t?.[ba]??null;return e||null}function gu(t){return t&&t.hasOwnProperty(ha)?t[ha]:null}function Au(t){return t&&!!t.\u0275providers}function Vu(t){return _a(t,"@NgModule"),t[$u]||null}function _n(t){return _a(t,"@Component"),t[Ou]||null}function Fu(t){return _a(t,"@Directive"),t[Pu]||null}function Am(t){return _a(t,"@Pipe"),t[Lu]||null}function _a(t,e){if(t==null)throw new x(-919,!1)}function Om(t){return typeof t=="string"?t:t==null?"":String(t)}function ju(t,e){return Lm("",-200,e)}function Ea(t,e){throw new x(-201,!1)}function Lm(t,e,n){let r=new x(e,t);return r[Pm]=e,r[PI]=t,n&&(r[LI]=n),r}function $I(t){return t[Pm]}function $m(){return vu}function it(t){let e=vu;return vu=t,e}function Uu(t,e,n){let r=oi(t);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(n&8)return null;if(e!==void 0)return e;Ea(t,"")}function jI(t,e=0){let n=fa();if(n===void 0)throw new x(-203,!1);if(n===null)return Uu(t,void 0,e);{let r=UI(e),o=n.retrieve(t,r);if(Wr(o)){if(r.optional)return null;throw o}return o}}function F(t,e=0){return($m()||jI)(Xe(t),e)}function y(t,e){return F(t,Zn(e))}function Zn(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function UI(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function bu(t){let e=[];for(let n=0;n<t.length;n++){let r=Xe(t[n]);if(Array.isArray(r)){if(r.length===0)throw new x(900,!1);let o,i=0;for(let s=0;s<r.length;s++){let a=r[s],c=HI(a);typeof c=="number"?c===-1?o=a.token:i|=c:o=a}e.push(F(o,i))}else e.push(F(r))}return e}function HI(t){return t[FI]}function Yn(t,e){let n=t.hasOwnProperty(Xo);return n?t[Xo]:null}function wa(t,e){t.forEach(n=>Array.isArray(n)?wa(n,e):e(n))}function Hu(t,e,n){e>=t.length?t.push(n):t.splice(e,0,n)}function ii(t,e){return e>=t.length-1?t.pop():t.splice(e,1)[0]}function Vm(t,e,n,r){let o=t.length;if(o==e)t.push(n,r);else if(o===1)t.push(r,t[0]),t[0]=n;else{for(o--,t.push(t[o-1],t[o]);o>e;){let i=o-2;t[o]=t[i],o--}t[e]=n,t[e+1]=r}}function Fm(t,e,n){let r=Qr(t,e);return r>=0?t[r|1]=n:(r=~r,Vm(t,r,e,n)),r}function Ia(t,e){let n=Qr(t,e);if(n>=0)return t[n|1]}function Qr(t,e){return BI(t,e,1)}function BI(t,e,n){let r=0,o=t.length>>n;for(;o!==r;){let i=r+(o-r>>1),s=t[i<<n];if(e===s)return i<<n;s>e?o=i:r=i+1}return~(o<<n)}function Jt(t){return{\u0275providers:t}}function jm(t){return Jt([{provide:nr,multi:!0,useValue:t}])}function Um(...t){return{\u0275providers:qu(!0,t),\u0275fromNgModule:!0}}function qu(t,...e){let n=[],r=new Set,o,i=s=>{n.push(s)};return wa(e,s=>{let a=s;ma(a,i,[],r)&&(o||=[],o.push(a))}),o!==void 0&&Hm(o,i),n}function Hm(t,e){for(let n=0;n<t.length;n++){let{ngModule:r,providers:o}=t[n];Wu(o,i=>{e(i,r)})}}function ma(t,e,n,r){if(t=Xe(t),!t)return!1;let o=null,i=gu(t),s=!i&&_n(t);if(!i&&!s){let c=t.ngModule;if(i=gu(c),i)o=c;else return!1}else{if(s&&!s.standalone)return!1;o=t}let a=r.has(o);if(s){if(a)return!1;if(r.add(o),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)ma(l,e,n,r)}}else if(i){if(i.imports!=null&&!a){r.add(o);let l;wa(i.imports,u=>{ma(u,e,n,r)&&(l||=[],l.push(u))}),l!==void 0&&Hm(l,e)}if(!a){let l=Yn(o)||(()=>new o);e({provide:o,useFactory:l,deps:yn},o),e({provide:zu,useValue:o,multi:!0},o),e({provide:nr,useValue:()=>F(o),multi:!0},o)}let c=i.providers;if(c!=null&&!a){let l=t;Wu(c,u=>{e(u,l)})}}else return!1;return o!==t&&t.providers!==void 0}function Wu(t,e){for(let n of t)Au(n)&&(n=n.\u0275providers),Array.isArray(n)?Wu(n,e):e(n)}function Bm(t){return t!==null&&typeof t=="object"&&zI in t}function qI(t){return!!(t&&t.useExisting)}function WI(t){return!!(t&&t.useFactory)}function ga(t){return typeof t=="function"}function ai(){return mu===void 0&&(mu=new ei),mu}function _u(t){let e=oi(t),n=e!==null?e.factory:Yn(t);if(n!==null)return n;if(t instanceof k)throw new x(-204,!1);if(t instanceof Function)return GI(t);throw new x(-204,!1)}function GI(t){if(t.length>0)throw new x(-204,!1);let n=OI(t);return n!==null?()=>n.factory(t):()=>new t}function KI(t){if(Bm(t))return Gr(void 0,t.useValue);{let e=zm(t);return Gr(e,pa)}}function zm(t,e,n){let r;if(ga(t)){let o=Xe(t);return Yn(o)||_u(o)}else if(Bm(t))r=()=>Xe(t.useValue);else if(WI(t))r=()=>t.useFactory(...bu(t.deps||[]));else if(qI(t))r=(o,i)=>F(Xe(t.useExisting),i!==void 0&&i&8?8:void 0);else{let o=Xe(t&&(t.useClass||t.provide));if(QI(t))r=()=>new o(...bu(t.deps));else return Yn(o)||_u(o)}return r}function Jo(t){if(t.destroyed)throw new x(-205,!1)}function Gr(t,e,n=!1){return{factory:t,value:e,multi:n?[]:void 0}}function QI(t){return!!t.deps}function ZI(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function YI(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Eu(t,e){for(let n of t)Array.isArray(n)?Eu(n,e):n&&Au(n)?Eu(n.\u0275providers,e):e(n)}function $e(t,e){let n;t instanceof Jn?(Jo(t),n=t):n=new yu(t);let r,o=Ft(n),i=it(void 0);try{return e()}finally{Ft(o),it(i)}}function qm(){return $m()!==void 0||fa()!=null}function In(t){return Array.isArray(t)&&typeof t[Wm]=="object"}function xt(t){return Array.isArray(t)&&t[Wm]===!0}function Qu(t){return(t.flags&4)!==0}function ir(t){return t.componentOffset>-1}function Zu(t){return(t.flags&1)===1}function sr(t){return!!t.template}function eo(t){return(t[U]&512)!==0}function ar(t){return(t[U]&256)===256}function Dt(t){for(;Array.isArray(t);)t=t[Ct];return t}function Yu(t,e){return Dt(e[t])}function Xt(t,e){return Dt(e[t.index])}function Ju(t,e){return t.data[e]}function en(t,e){let n=e[t];return In(n)?n:n[Ct]}function Da(t){return(t[U]&128)===128}function Qm(t){return xt(t[Ne])}function pi(t,e){return e==null?null:t[e]}function Xu(t){t[or]=0}function ed(t){t[U]&1024||(t[U]|=1024,Da(t)&&mi(t))}function hi(t){return!!(t[U]&9216||t[at]?.dirty)}function Ta(t){t[jt].changeDetectionScheduler?.notify(8),t[U]&64&&(t[U]|=1024),hi(t)&&mi(t)}function mi(t){t[jt].changeDetectionScheduler?.notify(0);let e=bn(t);for(;e!==null&&!(e[U]&8192||(e[U]|=8192,!Da(e)));)e=bn(e)}function td(t,e){if(ar(t))throw new x(911,!1);t[Zt]===null&&(t[Zt]=[]),t[Zt].push(e)}function Zm(t,e){if(t[Zt]===null)return;let n=t[Zt].indexOf(e);n!==-1&&t[Zt].splice(n,1)}function bn(t){let e=t[Ne];return xt(e)?e[Ne]:e}function Ym(){return Y.lFrame.elementDepthCount}function Jm(){Y.lFrame.elementDepthCount++}function nd(){Y.lFrame.elementDepthCount--}function Xm(){return Y.bindingsEnabled}function eg(){return Y.skipHydrationRootTNode!==null}function rd(t){return Y.skipHydrationRootTNode===t}function od(){Y.skipHydrationRootTNode=null}function Re(){return Y.lFrame.lView}function to(){return Y.lFrame.tView}function tn(){let t=id();for(;t!==null&&t.type===64;)t=t.parent;return t}function id(){return Y.lFrame.currentTNode}function tg(){let t=Y.lFrame,e=t.currentTNode;return t.isParent?e:e.parent}function gi(t,e){let n=Y.lFrame;n.currentTNode=t,n.isParent=e}function sd(){return Y.lFrame.isParent}function ng(){Y.lFrame.isParent=!1}function ad(){return wu}function cd(t){let e=wu;return wu=t,e}function rg(t){return Y.lFrame.bindingIndex=t}function ld(){return Y.lFrame.bindingIndex++}function og(t){let e=Y.lFrame,n=e.bindingIndex;return e.bindingIndex=e.bindingIndex+t,n}function ig(){return Y.lFrame.inI18n}function sg(t,e){let n=Y.lFrame;n.bindingIndex=n.bindingRootIndex=t,Ma(e)}function ag(){return Y.lFrame.currentDirectiveIndex}function Ma(t){Y.lFrame.currentDirectiveIndex=t}function cg(t){let e=Y.lFrame.currentDirectiveIndex;return e===-1?null:t[e]}function ud(t){Y.lFrame.currentQueryIndex=t}function JI(t){let e=t[H];return e.type===2?e.declTNode:e.type===1?t[vt]:null}function dd(t,e,n){if(n&4){let o=e,i=t;for(;o=o.parent,o===null&&!(n&1);)if(o=JI(i),o===null||(i=i[Yr],o.type&10))break;if(o===null)return!1;e=o,t=i}let r=Y.lFrame=lg();return r.currentTNode=e,r.lView=t,!0}function Na(t){let e=lg(),n=t[H];Y.lFrame=e,e.currentTNode=n.firstChild,e.lView=t,e.tView=n,e.contextLView=t,e.bindingIndex=n.bindingStartIndex,e.inI18n=!1}function lg(){let t=Y.lFrame,e=t===null?null:t.child;return e===null?ug(t):e}function ug(t){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=e),e}function dg(){let t=Y.lFrame;return Y.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}function Ra(){let t=dg();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function cr(){return Y.lFrame.selectedIndex}function Sn(t){Y.lFrame.selectedIndex=t}function fg(){let t=Y.lFrame;return Ju(t.tView,t.selectedIndex)}function pd(){return Y.lFrame.currentNamespace}function hd(){return pg}function md(t){pg=t}function Iu(t,e=null,n=null,r){let o=gd(t,e,n,r);return o.resolveInjectorInitializers(),o}function gd(t,e=null,n=null,r,o=new Set){let i=[n||yn,Um(t)],s;return new Jn(i,e||ai(),s||null,o)}function XI(){return new Su(Re())}function va(...t){}function vd(t){let e,n;function r(){t=va;try{n!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(n),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{t(),r()}),typeof requestAnimationFrame=="function"&&(n=requestAnimationFrame(()=>{t(),r()})),()=>r()}function gg(t){return queueMicrotask(()=>t()),()=>{t=va}}function bd(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function nS(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function e(){vd(()=>{t.callbackScheduled=!1,xu(t),t.isCheckStableRunning=!0,bd(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{e()}):t._outer.run(()=>{e()}),xu(t)}function rS(t){let e=()=>{nS(t)},n=eS++;t._inner=t._inner.fork({name:"angular",properties:{[yd]:!0,[ti]:n,[ti+n]:!0},onInvokeTask:(r,o,i,s,a,c)=>{if(oS(c))return r.invokeTask(i,s,a,c);try{return Rm(t),r.invokeTask(i,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&e(),km(t)}},onInvoke:(r,o,i,s,a,c,l)=>{try{return Rm(t),r.invoke(i,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!iS(c)&&e(),km(t)}},onHasTask:(r,o,i,s)=>{r.hasTask(i,s),o===i&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,xu(t),bd(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(r,o,i,s)=>(r.handleError(i,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function xu(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Rm(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function km(t){t._nesting--,bd(t)}function oS(t){return vg(t,"__ignore_ng_zone__")}function iS(t){return vg(t,"__scheduler_tick__")}function vg(t,e){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[e]===!0}function _d(){return Jt([jm(()=>{y(sS)})])}function Fe(t,e){let[n,r,o]=Al(t,e?.equal),i=n,s=i[qe];return i.set=r,i.update=o,i.asReadonly=bg.bind(i),i}function bg(){let t=this[qe];if(t.readonlyFn===void 0){let e=()=>this();e[qe]=t,t.readonlyFn=e}return t.readonlyFn}var Mu,x,kI,ba,ha,k,Ou,Pu,Lu,$u,Xo,er,Mm,Pm,PI,LI,vu,VI,Qn,FI,yu,tr,yn,nr,Bu,zu,ei,zI,si,pa,Nm,mu,ye,Jn,Ct,H,U,Ne,gt,vt,ci,Sa,st,rr,jt,Ve,Zr,Gu,Yr,yt,Jr,or,li,En,Ku,Zt,Ca,ui,at,xa,Xr,bt,Wm,wn,di,fi,et,Gm,Km,Y,wu,fd,pg,mt,be,Cn,Su,hg,mg,xn,Cu,Ke,yd,ti,eS,Qe,tS,ni,Yt,nn,yg,sS,Kr,vi,Ed,wd,Du,Tu,ka=f(()=>{"use strict";Hn();Kn();hu();Dm();Tm();Mu="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",x=class extends Error{code;constructor(e,n){super(Xn(e,n)),this.code=e}};kI=ee({__forward_ref__:ee});ba=ee({\u0275prov:ee}),ha=ee({\u0275inj:ee}),k=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,n){this._desc=e,this.\u0275prov=void 0,typeof n=="number"?this.__NG_ELEMENT_ID__=n:n!==void 0&&(this.\u0275prov=A({token:this,providedIn:n.providedIn||"root",factory:n.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};Ou=ee({\u0275cmp:ee}),Pu=ee({\u0275dir:ee}),Lu=ee({\u0275pipe:ee}),$u=ee({\u0275mod:ee}),Xo=ee({\u0275fac:ee}),er=ee({__NG_ELEMENT_ID__:ee}),Mm=ee({__NG_ENV_ID__:ee});Pm=ee({ngErrorCode:ee}),PI=ee({ngErrorMessage:ee}),LI=ee({ngTokenPath:ee});VI={},Qn=VI,FI="__NG_DI_FLAG__",yu=class{injector;constructor(e){this.injector=e}retrieve(e,n){let r=Zn(n)||0;try{return this.injector.get(e,r&8?null:Qn,r)}catch(o){if(Wr(o))return o;throw o}}};tr={},yn=[],nr=new k(""),Bu=new k("",-1),zu=new k(""),ei=class{get(e,n=Qn){if(n===Qn){let o=Lm("",-201);throw o.name="\u0275NotFound",o}return n}};zI=ee({provide:String,useValue:ee});si=new k(""),pa={},Nm={};ye=class{},Jn=class extends ye{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,n,r,o){super(),this.parent=n,this.source=r,this.scopes=o,Eu(e,s=>this.processProvider(s)),this.records.set(Bu,Gr(void 0,this)),o.has("environment")&&this.records.set(ye,Gr(void 0,this));let i=this.records.get(si);i!=null&&typeof i.value=="string"&&this.scopes.add(i.value),this.injectorDefTypes=new Set(this.get(zu,yn,{self:!0}))}retrieve(e,n){let r=Zn(n)||0;try{return this.get(e,Qn,r)}catch(o){if(Wr(o))return o;throw o}}destroy(){Jo(this),this._destroyed=!0;let e=B(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let n=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of n)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),B(e)}}onDestroy(e){return Jo(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){Jo(this);let n=Ft(this),r=it(void 0),o;try{return e()}finally{Ft(n),it(r)}}get(e,n=Qn,r){if(Jo(this),e.hasOwnProperty(Mm))return e[Mm](this);let o=Zn(r),i,s=Ft(this),a=it(void 0);try{if(!(o&4)){let l=this.records.get(e);if(l===void 0){let u=YI(e)&&oi(e);u&&this.injectableDefInScope(u)?l=Gr(_u(e),pa):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,o)}let c=o&2?ai():this.parent;return n=o&8&&n===Qn?null:n,c.get(e,n)}catch(c){let l=$I(c);throw l===-200||l===-201?new x(l,null):c}finally{it(a),Ft(s)}}resolveInjectorInitializers(){let e=B(null),n=Ft(this),r=it(void 0),o;try{let i=this.get(nr,yn,{self:!0});for(let s of i)s()}finally{Ft(n),it(r),B(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=Xe(e);let n=ga(e)?e:Xe(e&&e.provide),r=KI(e);if(!ga(e)&&e.multi===!0){let o=this.records.get(n);o||(o=Gr(void 0,pa,!0),o.factory=()=>bu(o.multi),this.records.set(n,o)),n=e,o.multi.push(e)}this.records.set(n,r)}hydrate(e,n,r){let o=B(null);try{if(n.value===Nm)throw ju("");return n.value===pa&&(n.value=Nm,n.value=n.factory(void 0,r)),typeof n.value=="object"&&n.value&&ZI(n.value)&&this._ngOnDestroyHooks.add(n.value),n.value}finally{B(o)}}injectableDefInScope(e){if(!e.providedIn)return!1;let n=Xe(e.providedIn);return typeof n=="string"?n==="any"||this.scopes.has(n):this.injectorDefTypes.has(n)}removeOnDestroy(e){let n=this._onDestroyHooks.indexOf(e);n!==-1&&this._onDestroyHooks.splice(n,1)}};Ct=0,H=1,U=2,Ne=3,gt=4,vt=5,ci=6,Sa=7,st=8,rr=9,jt=10,Ve=11,Zr=12,Gu=13,Yr=14,yt=15,Jr=16,or=17,li=18,En=19,Ku=20,Zt=21,Ca=22,ui=23,at=24,xa=25,Xr=26,bt=27,Wm=1,wn=7,di=8,fi=9,et=10;Gm="svg",Km="math";Y={lFrame:ug(null),bindingsEnabled:!0,skipHydrationRootTNode:null},wu=!1;fd=dg;pg=!0;mt=class t{static THROW_IF_NOT_FOUND=Qn;static NULL=new ei;static create(e,n){if(Array.isArray(e))return Iu({name:""},n,e,"");{let r=e.name??"";return Iu({name:r},e.parent,e.providers,r)}}static \u0275prov=A({token:t,providedIn:"any",factory:()=>F(Bu)});static __NG_ELEMENT_ID__=-1},be=new k(""),Cn=(()=>{class t{static __NG_ELEMENT_ID__=XI;static __NG_ENV_ID__=n=>n}return t})(),Su=class extends Cn{_lView;constructor(e){super(),this._lView=e}get destroyed(){return ar(this._lView)}onDestroy(e){let n=this._lView;return td(n,e),()=>Zm(n,e)}};hg=!1,mg=new k(""),xn=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Te(!1);debugTaskTracker=y(mg,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new z(n=>{n.next(!1),n.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let n=this.taskId++;return this.pendingTasks.add(n),this.debugTaskTracker?.add(n),n}has(n){return this.pendingTasks.has(n)}remove(n){this.pendingTasks.delete(n),this.debugTaskTracker?.remove(n),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=A({token:t,providedIn:"root",factory:()=>new t})}return t})(),Cu=class extends me{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,qm()&&(this.destroyRef=y(Cn,{optional:!0})??void 0,this.pendingTasks=y(xn,{optional:!0})??void 0)}emit(e){let n=B(null);try{super.next(e)}finally{B(n)}}subscribe(e,n,r){let o=e,i=n||(()=>null),s=r;if(e&&typeof e=="object"){let c=e;o=c.next?.bind(c),i=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(i=this.wrapInTimeout(i),o&&(o=this.wrapInTimeout(o)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:o,error:i,complete:s});return e instanceof De&&e.add(a),a}wrapInTimeout(e){return n=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{e(n)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},Ke=Cu;yd="isAngularZone",ti=yd+"_ID",eS=0,Qe=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Ke(!1);onMicrotaskEmpty=new Ke(!1);onStable=new Ke(!1);onError=new Ke(!1);constructor(e){let{enableLongStackTrace:n=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:o=!1,scheduleInRootZone:i=hg}=e;if(typeof Zone>"u")throw new x(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),n&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!o&&r,s.shouldCoalesceRunChangeDetection=o,s.callbackScheduled=!1,s.scheduleInRootZone=i,rS(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(yd)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new x(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new x(909,!1)}run(e,n,r){return this._inner.run(e,n,r)}runTask(e,n,r,o){let i=this._inner,s=i.scheduleEventTask("NgZoneEvent: "+o,e,tS,va,va);try{return i.runTask(s,n,r)}finally{i.cancelTask(s)}}runGuarded(e,n,r){return this._inner.runGuarded(e,n,r)}runOutsideAngular(e){return this._outer.run(e)}},tS={};ni=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Ke;onMicrotaskEmpty=new Ke;onStable=new Ke;onError=new Ke;run(e,n,r){return e.apply(n,r)}runGuarded(e,n,r){return e.apply(n,r)}runOutsideAngular(e){return e()}runTask(e,n,r,o){return e.apply(n,r)}};Yt=class{_console=console;handleError(e){this._console.error("ERROR",e)}},nn=new k("",{factory:()=>{let t=y(Qe),e=y(ye),n;return r=>{t.runOutsideAngular(()=>{e.destroyed&&!n?setTimeout(()=>{throw r}):(n??=e.get(Yt),n.handleError(r))})}}}),yg={provide:nr,useValue:()=>{let t=y(Yt,{optional:!0})},multi:!0},sS=new k("",{factory:()=>{let t=y(be).defaultView;if(!t)return;let e=y(nn),n=i=>{e(i.reason),i.preventDefault()},r=i=>{i.error?e(i.error):e(new Error(i.message,{cause:i})),i.preventDefault()},o=()=>{t.addEventListener("unhandledrejection",n),t.addEventListener("error",r)};typeof Zone<"u"?Zone.root.run(o):o(),y(Cn).onDestroy(()=>{t.removeEventListener("error",r),t.removeEventListener("unhandledrejection",n)})}});Kr=class{},vi=new k("",{factory:()=>!0}),Ed=new k(""),wd=(()=>{class t{static \u0275prov=A({token:t,providedIn:"root",factory:()=>new Du})}return t})(),Du=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let n=e.zone,r=this.queues.get(n);r.has(e)&&(r.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let n=e.zone;this.queues.has(n)||this.queues.set(n,new Set);let r=this.queues.get(n);r.has(e)||r.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[n,r]of this.queues)n===null?e||=this.flushQueue(r):e||=n.run(()=>this.flushQueue(r));e||(this.dirtyEffectCount=0)}}flushQueue(e){let n=!1;for(let r of e)r.dirty&&(this.dirtyEffectCount--,n=!0,r.run());return n}},Tu=class{[qe];constructor(e){this[qe]=e}destroy(){this[qe].destroy()}}});var Aa=f(()=>{"use strict";dm();fm();lu();vm();ym();ca();la();_m();Em();uu();bm();wm();Im()});function Za(t){return{toString:t}.toString()}function _S(t){return typeof t=="function"}function Gg(t,e,n,r){e!==null?e.applyValueToInputSignal(e,r):t[n]=r}function Kg(t){return t.type.prototype.ngOnChanges&&(t.setInput=wS),ES}function ES(){let t=Zg(this),e=t?.current;if(e){let n=t.previous;if(n===tr)t.previous=e;else for(let r in e)n[r]=e[r];t.current=null,this.ngOnChanges(e)}}function wS(t,e,n,r,o){let i=this.declaredInputs[r],s=Zg(t)||IS(t,{previous:tr,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[i];a[i]=new Va(l&&l.currentValue,n,c===tr),Gg(t,e,o,n)}function Zg(t){return t[Qg]||null}function IS(t,e){return t[Qg]=e}function SS(t,e,n){let{ngOnChanges:r,ngOnInit:o,ngDoCheck:i}=e.type.prototype;if(r){let s=Kg(e);(n.preOrderHooks??=[]).push(t,s),(n.preOrderCheckHooks??=[]).push(t,s)}o&&(n.preOrderHooks??=[]).push(0-t,o),i&&((n.preOrderHooks??=[]).push(t,i),(n.preOrderCheckHooks??=[]).push(t,i))}function CS(t,e){for(let n=e.directiveStart,r=e.directiveEnd;n<r;n++){let i=t.data[n].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=i;s&&(t.contentHooks??=[]).push(-n,s),a&&((t.contentHooks??=[]).push(n,a),(t.contentCheckHooks??=[]).push(n,a)),c&&(t.viewHooks??=[]).push(-n,c),l&&((t.viewHooks??=[]).push(n,l),(t.viewCheckHooks??=[]).push(n,l)),u!=null&&(t.destroyHooks??=[]).push(n,u)}}function Pa(t,e,n){Yg(t,e,3,n)}function La(t,e,n,r){(t[U]&3)===n&&Yg(t,e,n,r)}function Id(t,e){let n=t[U];(n&3)===e&&(n&=16383,n+=1,t[U]=n)}function Yg(t,e,n,r){let o=r!==void 0?t[or]&65535:0,i=r??-1,s=e.length-1,a=0;for(let c=o;c<s;c++)if(typeof e[c+1]=="number"){if(a=e[c],r!=null&&a>=r)break}else e[c]<0&&(t[or]+=65536),(a<i||i==-1)&&(xS(t,n,e,c),t[or]=(t[or]&4294901760)+c+2),c++}function Eg(t,e){ce(re.LifecycleHookStart,t,e);let n=B(null);try{e.call(t)}finally{B(n),ce(re.LifecycleHookEnd,t,e)}}function xS(t,e,n,r){let o=n[r]<0,i=n[r+1],s=o?-n[r]:n[r],a=t[s];o?t[U]>>14<t[or]>>16&&(t[U]&3)===e&&(t[U]+=16384,Eg(a,i)):Eg(a,i)}function DS(t){return(t.flags&8)!==0}function TS(t){return(t.flags&16)!==0}function MS(t,e,n){let r=0;for(;r<n.length;){let o=n[r];if(typeof o=="number"){if(o!==0)break;r++;let i=n[r++],s=n[r++],a=n[r++];t.setAttribute(e,s,a,i)}else{let i=o,s=n[++r];RS(i)?t.setProperty(e,i,s):t.setAttribute(e,i,s),r++}}return r}function NS(t){return t===3||t===4||t===6}function RS(t){return t.charCodeAt(0)===64}function Gd(t,e){if(!(e===null||e.length===0))if(t===null||t.length===0)t=e.slice();else{let n=-1;for(let r=0;r<e.length;r++){let o=e[r];typeof o=="number"?n=o:n===0||(n===-1||n===2?wg(t,n,o,null,e[++r]):wg(t,n,o,null,null))}}return t}function wg(t,e,n,r,o){let i=0,s=t.length;if(e===-1)s=-1;else for(;i<t.length;){let a=t[i++];if(typeof a=="number"){if(a===e){s=-1;break}else if(a>e){s=i-1;break}}}for(;i<t.length;){let a=t[i];if(typeof a=="number")break;if(a===n){o!==null&&(t[i+1]=o);return}i++,o!==null&&i++}s!==-1&&(t.splice(s,0,e),i=s+1),t.splice(i++,0,n),o!==null&&t.splice(i++,0,o)}function Jg(t){return t!==ro}function Fa(t){return t&32767}function kS(t){return t>>16}function ja(t,e){let n=kS(t),r=e;for(;n>0;)r=r[Yr],n--;return r}function Ig(t){let e=Md;return Md=t,e}function PS(t,e,n){let r;typeof n=="string"?r=n.charCodeAt(0)||0:n.hasOwnProperty(er)&&(r=n[er]),r==null&&(r=n[er]=OS++);let o=r&Xg,i=1<<o;e.data[t+(o>>ev)]|=i}function tv(t,e){let n=nv(t,e);if(n!==-1)return n;let r=e[H];r.firstCreatePass&&(t.injectorIndex=e.length,Sd(r.data,t),Sd(e,null),Sd(r.blueprint,null));let o=Kd(t,e),i=t.injectorIndex;if(Jg(o)){let s=Fa(o),a=ja(o,e),c=a[H].data;for(let l=0;l<8;l++)e[i+l]=a[s+l]|c[s+l]}return e[i+8]=o,i}function Sd(t,e){t.push(0,0,0,0,0,0,0,0,e)}function nv(t,e){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||e[t.injectorIndex+8]===null?-1:t.injectorIndex}function Kd(t,e){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let n=0,r=null,o=e;for(;o!==null;){if(r=av(o),r===null)return ro;if(n++,o=o[Yr],r.injectorIndex!==-1)return r.injectorIndex|n<<16}return ro}function LS(t,e,n){PS(t,e,n)}function rv(t,e,n){if(n&8||t!==void 0)return t;Ea(e,"NodeInjector")}function ov(t,e,n,r){if(n&8&&r===void 0&&(r=null),(n&3)===0){let o=t[rr],i=it(void 0);try{return o?o.get(e,r,n&8):Uu(e,r,n&8)}finally{it(i)}}return rv(r,e,n)}function iv(t,e,n,r=0,o){if(t!==null){if(e[U]&2048&&!(r&2)){let s=US(t,e,n,r,Ut);if(s!==Ut)return s}let i=sv(t,e,n,r,Ut);if(i!==Ut)return i}return ov(e,n,r,o)}function sv(t,e,n,r,o){let i=FS(n);if(typeof i=="function"){if(!dd(e,t,r))return r&1?rv(o,n,r):ov(e,n,r,o);try{let s;if(s=i(r),s==null&&!(r&8))Ea(n);else return s}finally{fd()}}else if(typeof i=="number"){let s=null,a=nv(t,e),c=ro,l=r&1?e[yt][vt]:null;for((a===-1||r&4)&&(c=a===-1?Kd(t,e):e[a+8],c===ro||!Cg(r,!1)?a=-1:(s=e[H],a=Fa(c),e=ja(c,e)));a!==-1;){let u=e[H];if(Sg(i,a,u.data)){let d=$S(a,e,n,s,r,l);if(d!==Ut)return d}c=e[a+8],c!==ro&&Cg(r,e[H].data[a+8]===l)&&Sg(i,a,e)?(s=u,a=Fa(c),e=ja(c,e)):a=-1}}return o}function $S(t,e,n,r,o,i){let s=e[H],a=s.data[t+8],c=r==null?ir(a)&&Md:r!=s&&(a.type&3)!==0,l=o&1&&i===a,u=VS(a,s,n,c,l);return u!==null?Nd(e,s,u,a,o):Ut}function VS(t,e,n,r,o){let i=t.providerIndexes,s=e.data,a=i&1048575,c=t.directiveStart,l=t.directiveEnd,u=i>>20,d=r?a:a+u,h=o?a+u:l;for(let p=d;p<h;p++){let g=s[p];if(p<c&&n===g||p>=c&&g.type===n)return p}if(o){let p=s[c];if(p&&sr(p)&&p.type===n)return c}return null}function Nd(t,e,n,r,o){let i=t[n],s=e.data;if(i instanceof Ei){let a=i;if(a.resolving)throw ju("");let c=Ig(a.canSeeViewProviders);a.resolving=!0;let l=s[n].type||s[n],u,d=a.injectImpl?it(a.injectImpl):null,h=dd(t,r,0);try{i=t[n]=a.factory(void 0,o,s,t,r),e.firstCreatePass&&n>=r.directiveStart&&SS(n,s[n],e)}finally{d!==null&&it(d),Ig(c),a.resolving=!1,fd()}}return i}function FS(t){if(typeof t=="string")return t.charCodeAt(0)||0;let e=t.hasOwnProperty(er)?t[er]:void 0;return typeof e=="number"?e>=0?e&Xg:jS:e}function Sg(t,e,n){let r=1<<t;return!!(n[e+(t>>ev)]&r)}function Cg(t,e){return!(t&2)&&!(t&1&&e)}function jS(){return new lr(tn(),Re())}function xi(t){return Za(()=>{let e=t.prototype.constructor,n=e[Xo]||Rd(e),r=Object.prototype,o=Object.getPrototypeOf(t.prototype).constructor;for(;o&&o!==r;){let i=o[Xo]||Rd(o);if(i&&i!==n)return i;o=Object.getPrototypeOf(o)}return i=>new i})}function Rd(t){return Ru(t)?()=>{let e=Rd(Xe(t));return e&&e()}:Yn(t)}function US(t,e,n,r,o){let i=t,s=e;for(;i!==null&&s!==null&&s[U]&2048&&!eo(s);){let a=sv(i,s,n,r|2,Ut);if(a!==Ut)return a;let c=i.parent;if(!c){let l=s[Ku];if(l){let u=l.get(n,Ut,r&-5);if(u!==Ut)return u}c=av(s),s=s[Yr]}i=c}return o}function av(t){let e=t[H],n=e.type;return n===2?e.declTNode:n===1?t[vt]:null}function HS(){return Qd(tn(),Re())}function Qd(t,e){return new cv(Xt(t,e))}function lv(t){return(t.flags&128)===128}function zS(){return BS++}function qS(t){uv.set(t[En],t)}function kd(t){uv.delete(t[En])}function wi(t,e){In(e)?(t[xg]=e[En],qS(e)):t[xg]=e}function dv(t){return pv(t[Zr])}function fv(t){return pv(t[gt])}function pv(t){for(;t!==null&&!xt(t);)t=t[gt];return t}function Yd(t){WS=t}function Jd(t){return(t.flags&32)===32}function gv(t,e,n=!1){return KS(t,e,n)}function vv(t,e){let n=t.contentQueries;if(n!==null){let r=B(null);try{for(let o=0;o<n.length;o+=2){let i=n[o],s=n[o+1];if(s!==-1){let a=t.data[s];ud(i),a.contentQueries(2,e[s],s)}}}finally{B(r)}}}function Ad(t,e,n){ud(0);let r=B(null);try{e(t,n)}finally{B(r)}}function yv(t,e,n){if(Qu(e)){let r=B(null);try{let o=e.directiveStart,i=e.directiveEnd;for(let s=o;s<i;s++){let a=t.data[s];if(a.contentQueries){let c=n[s];a.contentQueries(1,c,s)}}}finally{B(r)}}}function bv(t){return t instanceof Od?t.changingThisBreaksApplicationSecurity:t}function QS(t,e){return t.createText(e)}function ZS(t,e,n){t.setValue(e,n)}function _v(t,e,n){return t.createElement(e,n)}function Ua(t,e,n,r,o){t.insertBefore(e,n,r,o)}function Ev(t,e,n){t.appendChild(e,n)}function Dg(t,e,n,r,o){r!==null?Ua(t,e,n,r,o):Ev(t,e,n)}function YS(t,e,n,r){t.removeChild(null,e,n,r)}function JS(t,e,n){t.setAttribute(e,"style",n)}function XS(t,e,n){n===""?t.removeAttribute(e,"class"):t.setAttribute(e,"class",n)}function wv(t,e,n){let{mergedAttrs:r,classes:o,styles:i}=n;r!==null&&MS(t,e,r),o!==null&&XS(t,e,o),i!==null&&JS(t,e,i)}function Iv(t){return t instanceof Function?t():t}function eC(t,e,n){let r=t.length;for(;;){let o=t.indexOf(e,n);if(o===-1)return o;if(o===0||t.charCodeAt(o-1)<=32){let i=e.length;if(o+i===r||t.charCodeAt(o+i)<=32)return o}n=o+1}}function tC(t,e,n,r){let o=0;if(r){for(;o<e.length&&typeof e[o]=="string";o+=2)if(e[o]==="class"&&eC(e[o+1].toLowerCase(),n,0)!==-1)return!0}else if(Xd(t))return!1;if(o=e.indexOf(1,o),o>-1){let i;for(;++o<e.length&&typeof(i=e[o])=="string";)if(i.toLowerCase()===n)return!0}return!1}function Xd(t){return t.type===4&&t.value!==Sv}function nC(t,e,n){let r=t.type===4&&!n?Sv:t.value;return e===r}function rC(t,e,n){let r=4,o=t.attrs,i=o!==null?sC(o):0,s=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!s&&!Tt(r)&&!Tt(c))return!1;if(s&&Tt(c))continue;s=!1,r=c|r&1;continue}if(!s)if(r&4){if(r=2|r&1,c!==""&&!nC(t,c,n)||c===""&&e.length===1){if(Tt(r))return!1;s=!0}}else if(r&8){if(o===null||!tC(t,o,c,n)){if(Tt(r))return!1;s=!0}}else{let l=e[++a],u=oC(c,o,Xd(t),n);if(u===-1){if(Tt(r))return!1;s=!0;continue}if(l!==""){let d;if(u>i?d="":d=o[u+1].toLowerCase(),r&2&&l!==d){if(Tt(r))return!1;s=!0}}}}return Tt(r)||s}function Tt(t){return(t&1)===0}function oC(t,e,n,r){if(e===null)return-1;let o=0;if(r||!n){let i=!1;for(;o<e.length;){let s=e[o];if(s===t)return o;if(s===3||s===6)i=!0;else if(s===1||s===2){let a=e[++o];for(;typeof a=="string";)a=e[++o];continue}else{if(s===4)break;if(s===0){o+=4;continue}}o+=i?1:2}return-1}else return aC(e,t)}function iC(t,e,n=!1){for(let r=0;r<e.length;r++)if(rC(t,e[r],n))return!0;return!1}function sC(t){for(let e=0;e<t.length;e++){let n=t[e];if(NS(n))return e}return t.length}function aC(t,e){let n=t.indexOf(4);if(n>-1)for(n++;n<t.length;){let r=t[n];if(typeof r=="number")return-1;if(r===e)return n;n++}return-1}function Tg(t,e){return t?":not("+e.trim()+")":e}function cC(t){let e=t[0],n=1,r=2,o="",i=!1;for(;n<t.length;){let s=t[n];if(typeof s=="string")if(r&2){let a=t[++n];o+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else r&8?o+="."+s:r&4&&(o+=" "+s);else o!==""&&!Tt(s)&&(e+=Tg(i,o),o=""),r=s,i=i||!Tt(r);n++}return o!==""&&(e+=Tg(i,o)),e}function lC(t){return t.map(cC).join(",")}function uC(t){let e=[],n=[],r=1,o=2;for(;r<t.length;){let i=t[r];if(typeof i=="string")o===2?i!==""&&e.push(i,t[++r]):o===8&&n.push(i);else{if(!Tt(o))break;o=i}r++}return n.length&&e.push(1,...n),e}function Cv(t,e,n,r,o,i,s,a,c,l,u){let d=bt+r,h=d+o,p=dC(d,h),g=typeof l=="function"?l():l;return p[H]={type:t,blueprint:p,template:n,queries:null,viewQuery:a,declTNode:e,data:p.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof i=="function"?i():i,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:u}}function dC(t,e){let n=[];for(let r=0;r<e;r++)n.push(r<t?null:Dn);return n}function fC(t){let e=t.tView;return e===null||e.incompleteFirstPass?t.tView=Cv(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):e}function xv(t,e,n,r,o,i,s,a,c,l,u){let d=e.blueprint.slice();return d[Ct]=o,d[U]=r|4|128|8|64|1024,(l!==null||t&&t[U]&2048)&&(d[U]|=2048),Xu(d),d[Ne]=d[Yr]=t,d[st]=n,d[jt]=s||t&&t[jt],d[Ve]=a||t&&t[Ve],d[rr]=c||t&&t[rr]||null,d[vt]=i,d[En]=zS(),d[ci]=u,d[Ku]=l,d[yt]=e.type==2?t[yt]:d,d}function pC(t,e,n){let r=Xt(e,t),o=fC(n),i=t[jt].rendererFactory,s=Mv(t,xv(t,o,null,Dv(n),r,e,null,i.createRenderer(r,n),null,null,null));return t[e.index]=s}function Dv(t){let e=16;return t.signals?e=4096:t.onPush&&(e=64),e}function Tv(t,e,n,r){if(n===0)return-1;let o=e.length;for(let i=0;i<n;i++)e.push(r),t.blueprint.push(r),t.data.push(null);return o}function Mv(t,e){return t[Zr]?t[Gu][gt]=e:t[Zr]=e,t[Gu]=e,e}function co(t=1){Nv(to(),Re(),cr()+t,!1)}function Nv(t,e,n,r){if(!r)if((e[U]&3)===3){let i=t.preOrderCheckHooks;i!==null&&Pa(e,i,n)}else{let i=t.preOrderHooks;i!==null&&La(e,i,0,n)}Sn(n)}function Pd(t,e,n,r){let o=B(null);try{let[i,s,a]=t.inputs[n],c=null;(s&ec.SignalBased)!==0&&(c=e[i][qe]),c!==null&&c.transformFn!==void 0?r=c.transformFn(r):a!==null&&(r=a.call(e,r)),t.setInput!==null?t.setInput(e,c,r,n,i):Gg(e,c,i,r)}finally{B(o)}}function ef(t,e){return hC(t,e)}function mC(t,e){let n=Ld.get(t);if(!n||n.length===0)return;let r=e.parentNode,o=e.previousSibling;for(let i=n.length-1;i>=0;i--){let s=n[i],a=s.parentNode;s===e?(n.splice(i,1),yi.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(o&&s===o||a&&r&&a!==r)&&(n.splice(i,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function gC(t,e){let n=Ld.get(t);n?n.includes(e)||n.push(e):Ld.set(t,[e])}function nf(t){Mg.has(t)||(Mg.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}function kv(t,e,n){let r=t.get(vC);if(Array.isArray(e))for(let o of e)r.queue.add(o),n?.detachedLeaveAnimationFns?.push(o);else r.queue.add(e),n?.detachedLeaveAnimationFns?.push(e);r.scheduler&&r.scheduler(t)}function yC(t,e){for(let[n,r]of e)kv(t,r.animateFns)}function Ng(t,e,n,r){let o=t?.[Xr]?.enter;e!==null&&o&&o.has(n.index)&&yC(r,o)}function no(t,e,n,r,o,i,s,a){if(o!=null){let c,l=!1;xt(o)?c=o:In(o)&&(l=!0,o=o[Ct]);let u=Dt(o);t===0&&r!==null?(Ng(a,r,i,n),s==null?Ev(e,r,u):Ua(e,r,u,s||null,!0)):t===1&&r!==null?(Ng(a,r,i,n),Ua(e,r,u,s||null,!0),mC(i,u)):t===2?(a?.[Xr]?.leave?.has(i.index)&&gC(i,u),yi.delete(u),Rg(a,i,n,d=>{if(yi.has(u)){yi.delete(u);return}YS(e,u,l,d)})):t===3&&(yi.delete(u),Rg(a,i,n,()=>{e.destroyNode(u)})),c!=null&&RC(e,t,n,c,i,r,s)}}function bC(t,e){Av(t,e),e[Ct]=null,e[vt]=null}function _C(t,e,n,r,o,i){r[Ct]=o,r[vt]=e,tc(t,r,n,1,o,i)}function Av(t,e){e[jt].changeDetectionScheduler?.notify(9),tc(t,e,e[Ve],2,null,null)}function EC(t){let e=t[Zr];if(!e)return Cd(t[H],t);for(;e;){let n=null;if(In(e))n=e[Zr];else{let r=e[et];r&&(n=r)}if(!n){for(;e&&!e[gt]&&e!==t;)In(e)&&Cd(e[H],e),e=e[Ne];e===null&&(e=t),In(e)&&Cd(e[H],e),n=e&&e[gt]}e=n}}function rf(t,e){let n=t[fi],r=n.indexOf(e);n.splice(r,1)}function Ov(t,e){if(ar(e))return;let n=e[Ve];n.destroyNode&&tc(t,e,n,3,null,null),EC(e)}function Cd(t,e){if(ar(e))return;let n=B(null);try{e[U]&=-129,e[U]|=256,e[at]&&Bo(e[at]),SC(t,e),IC(t,e),e[H].type===1&&e[Ve].destroy();let r=e[Jr];if(r!==null&&xt(e[Ne])){r!==e[Ne]&&rf(r,e);let o=e[li];o!==null&&o.detachView(t)}kd(e)}finally{B(n)}}function Rg(t,e,n,r){let o=t?.[Xr];if(o==null||o.leave==null||!o.leave.has(e.index))return r(!1);t&&oo.add(t[En]),kv(n,()=>{if(o.leave&&o.leave.has(e.index)){let s=o.leave.get(e.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:u}=l();a.push(u)}o.detachedLeaveAnimationFns=void 0}o.running=Promise.allSettled(a),wC(t,r)}else t&&oo.delete(t[En]),r(!1)},o)}function wC(t,e){let n=t[Xr]?.running;if(n){n.then(()=>{t[Xr].running=void 0,oo.delete(t[En]),e(!0)});return}e(!1)}function IC(t,e){let n=t.cleanup,r=e[Sa];if(n!==null)for(let s=0;s<n.length-1;s+=2)if(typeof n[s]=="string"){let a=n[s+3];a>=0?r[a]():r[-a].unsubscribe(),s+=2}else{let a=r[n[s+1]];n[s].call(a)}r!==null&&(e[Sa]=null);let o=e[Zt];if(o!==null){e[Zt]=null;for(let s=0;s<o.length;s++){let a=o[s];a()}}let i=e[ui];if(i!==null){e[ui]=null;for(let s of i)s.destroy()}}function SC(t,e){let n;if(t!=null&&(n=t.destroyHooks)!=null)for(let r=0;r<n.length;r+=2){let o=e[n[r]];if(!(o instanceof Ei)){let i=n[r+1];if(Array.isArray(i))for(let s=0;s<i.length;s+=2){let a=o[i[s]],c=i[s+1];ce(re.LifecycleHookStart,a,c);try{c.call(a)}finally{ce(re.LifecycleHookEnd,a,c)}}else{ce(re.LifecycleHookStart,o,i);try{i.call(o)}finally{ce(re.LifecycleHookEnd,o,i)}}}}}function CC(t,e,n){return xC(t,e.parent,n)}function xC(t,e,n){let r=e;for(;r!==null&&r.type&168;)e=r,r=e.parent;if(r===null)return n[Ct];if(ir(r)){let{encapsulation:o}=t.data[r.directiveStart+r.componentOffset];if(o===Mt.None||o===Mt.Emulated)return null}return Xt(r,n)}function DC(t,e,n){return MC(t,e,n)}function TC(t,e,n){return t.type&40?Xt(t,n):null}function Pv(t,e,n,r){let o=CC(t,r,e),i=e[Ve],s=r.parent||e[vt],a=DC(s,r,e);if(o!=null)if(Array.isArray(n))for(let c=0;c<n.length;c++)Dg(i,o,n[c],a,!1);else Dg(i,o,n,a,!1);kg!==void 0&&kg(i,r,e,n,o)}function bi(t,e){if(e!==null){let n=e.type;if(n&3)return Xt(e,t);if(n&4)return $d(-1,t[e.index]);if(n&8){let r=e.child;if(r!==null)return bi(t,r);{let o=t[e.index];return xt(o)?$d(-1,o):Dt(o)}}else{if(n&128)return bi(t,e.next);if(n&32)return ef(e,t)()||Dt(t[e.index]);{let r=Lv(t,e);if(r!==null){if(Array.isArray(r))return r[0];let o=bn(t[yt]);return bi(o,r)}else return bi(t,e.next)}}}return null}function Lv(t,e){if(e!==null){let r=t[yt][vt],o=e.projection;return r.projection[o]}return null}function $d(t,e){let n=et+t+1;if(n<e.length){let r=e[n],o=r[H].firstChild;if(o!==null)return bi(r,o)}return e[wn]}function of(t,e,n,r,o,i,s){for(;n!=null;){let a=r[rr];if(n.type===128){n=n.next;continue}let c=r[n.index],l=n.type;if(s&&e===0&&(c&&wi(Dt(c),r),n.flags|=2),!Jd(n))if(l&8)of(t,e,n.child,r,o,i,!1),no(e,t,a,o,c,n,i,r);else if(l&32){let u=ef(n,r),d;for(;d=u();)no(e,t,a,o,d,n,i,r);no(e,t,a,o,c,n,i,r)}else l&16?NC(t,e,r,n,o,i):no(e,t,a,o,c,n,i,r);n=s?n.projectionNext:n.next}}function tc(t,e,n,r,o,i){of(n,r,t.firstChild,e,o,i,!1)}function NC(t,e,n,r,o,i){let s=n[yt],c=s[vt].projection[r.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];no(e,t,n[rr],o,u,r,i,n)}else{let l=c,u=s[Ne];lv(r)&&(l.flags|=128),of(t,e,l,u,o,i,!0)}}function RC(t,e,n,r,o,i,s){let a=r[wn],c=Dt(r);a!==c&&no(e,t,n,i,a,o,s);for(let l=et;l<r.length;l++){let u=r[l];tc(u[H],u,t,e,i,a)}}function kC(t,e,n,r,o){if(e)o?t.addClass(n,r):t.removeClass(n,r);else{let i=r.indexOf("-")===-1?void 0:rn.DashCase;o==null?t.removeStyle(n,r,i):(typeof o=="string"&&o.endsWith("!important")&&(o=o.slice(0,-10),i|=rn.Important),t.setStyle(n,r,o,i))}}function $v(t,e,n,r,o){let i=cr(),s=r&2;try{Sn(-1),s&&e.length>bt&&Nv(t,e,bt,!1);let a=s?re.TemplateUpdateStart:re.TemplateCreateStart;ce(a,o,n),n(r,o)}finally{Sn(i);let a=s?re.TemplateUpdateEnd:re.TemplateCreateEnd;ce(a,o,n)}}function Vv(t,e,n){$C(t,e,n),(n.flags&64)===64&&VC(t,e,n)}function Fv(t,e,n=Xt){let r=e.localNames;if(r!==null){let o=e.index+1;for(let i=0;i<r.length;i+=2){let s=r[i+1],a=s===-1?n(e,t):t[s];t[o++]=a}}}function AC(t,e,n,r){let i=r.get(mv,hv)||n===Mt.ShadowDom||n===Mt.ExperimentalIsolatedShadowDom,s=t.selectRootElement(e,i);return OC(s),s}function OC(t){PC(t)}function LC(t,e,n,r,o,i){if(t.type&3){let s=Xt(t,e);r=i!=null?i(r,t.value||"",n):r,o.setProperty(s,n,r)}else t.type&12}function $C(t,e,n){let r=n.directiveStart,o=n.directiveEnd;ir(n)&&pC(e,n,t.data[r+n.componentOffset]),t.firstCreatePass||tv(n,e);let i=n.initialInputs;for(let s=r;s<o;s++){let a=t.data[s],c=Nd(e,t,s,n);if(wi(c,e),i!==null&&UC(e,s-r,c,a,n,i),sr(a)){let l=en(n.index,e);l[st]=Nd(e,t,s,n)}}}function VC(t,e,n){let r=n.directiveStart,o=n.directiveEnd,i=n.index,s=ag();try{Sn(i);for(let a=r;a<o;a++){let c=t.data[a],l=e[a];Ma(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&FC(c,l)}}finally{Sn(-1),Ma(s)}}function FC(t,e){t.hostBindings!==null&&t.hostBindings(1,e)}function jC(t,e){let n=t.directiveRegistry,r=null;if(n)for(let o=0;o<n.length;o++){let i=n[o];iC(e,i.selectors,!1)&&(r??=[],sr(i)?r.unshift(i):r.push(i))}return r}function UC(t,e,n,r,o,i){let s=i[e];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Pd(r,n,c,l)}}function jv(t,e,n,r,o){let i=bt+n,s=e[H],a=o(s,e,t,r,n);e[i]=a,gi(t,!0);let c=t.type===2;return c?(wv(e[Ve],a,t),(Ym()===0||Zu(t))&&wi(a,e),Jm()):wi(a,e),hd()&&(!c||!Jd(t))&&Pv(s,e,a,t),t}function Uv(t){let e=t;return sd()?ng():(e=e.parent,gi(e,!1)),e}function Hv(t,e,n,r,o){let i=t.inputs?.[r],s=t.hostDirectiveInputs?.[r],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],u=s[c+1],d=e.data[l];Pd(d,n[l],u,o),a=!0}if(i)for(let c of i){let l=n[c],u=e.data[c];Pd(u,l,r,o),a=!0}return a}function HC(t,e){let n=en(e,t),r=n[H];BC(r,n);let o=n[Ct];o!==null&&n[ci]===null&&(n[ci]=gv(o,n[rr])),ce(re.ComponentStart);try{Bv(r,n,n[st])}finally{ce(re.ComponentEnd,n[st])}}function BC(t,e){for(let n=e.length;n<t.blueprint.length;n++)e.push(t.blueprint[n])}function Bv(t,e,n){Na(e);try{let r=t.viewQuery;r!==null&&Ad(1,r,n);let o=t.template;o!==null&&$v(t,e,o,1,n),t.firstCreatePass&&(t.firstCreatePass=!1),e[li]?.finishViewCreation(t),t.staticContentQueries&&vv(t,e),t.staticViewQueries&&Ad(2,t.viewQuery,n);let i=t.components;i!==null&&zC(e,i)}catch(r){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),r}finally{e[U]&=-5,Ra()}}function zC(t,e){for(let n=0;n<e.length;n++)HC(t,e[n])}function Ag(t,e){return!e||e.firstChild===null||lv(t)}function Ii(t,e,n,r,o=!1){for(;n!==null;){if(n.type===128){n=o?n.projectionNext:n.next;continue}let i=e[n.index];i!==null&&r.push(Dt(i)),xt(i)&&zv(i,r);let s=n.type;if(s&8)Ii(t,e,n.child,r);else if(s&32){let a=ef(n,e),c;for(;c=a();)r.push(c)}else if(s&16){let a=Lv(e,n);if(Array.isArray(a))r.push(...a);else{let c=bn(e[yt]);Ii(c[H],c,a,r,!0)}}n=o?n.projectionNext:n.next}return r}function zv(t,e){for(let n=et;n<t.length;n++){let r=t[n],o=r[H].firstChild;o!==null&&Ii(r[H],r,o,e)}t[wn]!==t[Ct]&&e.push(t[wn])}function qv(t){if(t[xa]!==null){for(let e of t[xa])e.impl.addSequence(e);t[xa].length=0}}function qC(t){return t[at]??WC(t)}function WC(t){let e=Wv.pop()??Object.create(KC);return e.lView=t,e}function GC(t){t.lView[at]!==t&&(t.lView=null,Wv.push(t))}function QC(t){let e=t[at]??Object.create(ZC);return e.lView=t,e}function Gv(t){return t.type!==2}function Kv(t){if(t[ui]===null)return;let e=!0;for(;e;){let n=!1;for(let r of t[ui])r.dirty&&(n=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()));e=n&&!!(t[U]&8192)}}function Qv(t,e=0){let r=t[jt].rendererFactory,o=!1;o||r.begin?.();try{JC(t,e)}finally{o||r.end?.()}}function JC(t,e){let n=ad();try{cd(!0),Vd(t,e);let r=0;for(;hi(t);){if(r===YC)throw new x(103,!1);r++,Vd(t,1)}}finally{cd(n)}}function XC(t,e,n,r){if(ar(e))return;let o=e[U],i=!1,s=!1;Na(e);let a=!0,c=null,l=null;i||(Gv(t)?(l=qC(e),c=Pr(l)):Ns()===null?(a=!1,l=QC(e),c=Pr(l)):e[at]&&(Bo(e[at]),e[at]=null));try{Xu(e),rg(t.bindingStartIndex),n!==null&&$v(t,e,n,2,r);let u=(o&3)===3;if(!i)if(u){let p=t.preOrderCheckHooks;p!==null&&Pa(e,p,null)}else{let p=t.preOrderHooks;p!==null&&La(e,p,0,null),Id(e,0)}if(s||ex(e),Kv(e),Zv(e,0),t.contentQueries!==null&&vv(t,e),!i)if(u){let p=t.contentCheckHooks;p!==null&&Pa(e,p)}else{let p=t.contentHooks;p!==null&&La(e,p,1),Id(e,1)}nx(t,e);let d=t.components;d!==null&&Jv(e,d,0);let h=t.viewQuery;if(h!==null&&Ad(2,h,r),!i)if(u){let p=t.viewCheckHooks;p!==null&&Pa(e,p)}else{let p=t.viewHooks;p!==null&&La(e,p,2),Id(e,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),e[Ca]){for(let p of e[Ca])p();e[Ca]=null}i||(qv(e),e[U]&=-73)}catch(u){throw i||mi(e),u}finally{l!==null&&(Ho(l,c),a&&GC(l)),Ra()}}function Zv(t,e){for(let n=dv(t);n!==null;n=fv(n))for(let r=et;r<n.length;r++){let o=n[r];Yv(o,e)}}function ex(t){for(let e=dv(t);e!==null;e=fv(e)){if(!(e[U]&2))continue;let n=e[fi];for(let r=0;r<n.length;r++){let o=n[r];ed(o)}}}function tx(t,e,n){ce(re.ComponentStart);let r=en(e,t);try{Yv(r,n)}finally{ce(re.ComponentEnd,r[st])}}function Yv(t,e){Da(t)&&Vd(t,e)}function Vd(t,e){let r=t[H],o=t[U],i=t[at],s=!!(e===0&&o&16);if(s||=!!(o&64&&e===0),s||=!!(o&1024),s||=!!(i?.dirty&&ks(i)),s||=!1,i&&(i.dirty=!1),t[U]&=-9217,s)XC(r,t,r.template,t[st]);else if(o&8192){let a=B(null);try{Kv(t),Zv(t,1);let c=r.components;c!==null&&Jv(t,c,1),qv(t)}finally{B(a)}}}function Jv(t,e,n){for(let r=0;r<e.length;r++)tx(t,e[r],n)}function nx(t,e){let n=t.hostBindingOpCodes;if(n!==null)try{for(let r=0;r<n.length;r++){let o=n[r];if(o<0)Sn(~o);else{let i=o,s=n[++r],a=n[++r];sg(s,i);let c=e[i];ce(re.HostBindingsUpdateStart,c);try{a(2,c)}finally{ce(re.HostBindingsUpdateEnd,c)}}}}finally{Sn(-1)}}function Xv(t,e){let n=ad()?64:1088;for(t[jt].changeDetectionScheduler?.notify(e);t;){t[U]|=n;let r=bn(t);if(eo(t)&&!r)return t;t=r}return null}function rx(t,e,n,r){return[t,!0,0,e,null,r,null,n,null,null]}function ox(t,e,n,r=!0){let o=e[H];if(ix(o,e,t,n),r){let s=$d(n,t),a=e[Ve],c=a.parentNode(t[wn]);c!==null&&_C(o,t[vt],a,e,c,s)}let i=e[ci];i!==null&&i.firstChild!==null&&(i.firstChild=null)}function Fd(t,e){if(t.length<=et)return;let n=et+e,r=t[n];if(r){let o=r[Jr];o!==null&&o!==t&&rf(o,r),e>0&&(t[n-1][gt]=r[gt]);let i=ii(t,et+e);bC(r[H],r);let s=i[li];s!==null&&s.detachView(i[H]),r[Ne]=null,r[gt]=null,r[U]&=-129}return r}function ix(t,e,n,r){let o=et+r,i=n.length;r>0&&(n[o-1][gt]=e),r<i-et?(e[gt]=n[o],Hu(n,et+r,e)):(n.push(e),e[gt]=null),e[Ne]=n;let s=e[Jr];s!==null&&n!==s&&ey(s,e);let a=e[li];a!==null&&a.insertView(t),Ta(e),e[U]|=128}function ey(t,e){let n=t[fi],r=e[Ne];if(In(r))t[U]|=2;else{let o=r[Ne][yt];e[yt]!==o&&(t[U]|=2)}n===null?t[fi]=[e]:n.push(e)}function sf(t,e,n,r,o){let i=t.data[e];if(i===null)i=sx(t,e,n,r,o),ig()&&(i.flags|=32);else if(i.type&64){i.type=n,i.value=r,i.attrs=o;let s=tg();i.injectorIndex=s===null?-1:s.injectorIndex}return gi(i,!0),i}function sx(t,e,n,r,o){let i=id(),s=sd(),a=s?i:i&&i.parent,c=t.data[e]=cx(t,a,n,e,r,o);return ax(t,c,i,s),c}function ax(t,e,n,r){t.firstChild===null&&(t.firstChild=e),n!==null&&(r?n.child==null&&e.parent!==null&&(n.child=e):n.next===null&&(n.next=e,e.prev=n))}function cx(t,e,n,r,o,i){let s=e?e.injectorIndex:-1,a=0;return eg()&&(a|=128),{type:n,index:r,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:o,namespace:pd(),attrs:i,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function Og(t,e){return lx(t,e)}function Ha(t,e,n){let r=n?t.styles:null,o=n?t.classes:null,i=0;if(e!==null)for(let s=0;s<e.length;s++){let a=e[s];if(typeof a=="number")i=a;else if(i==1)o=Nu(o,a);else if(i==2){let c=a,l=e[++s];r=Nu(r,c+": "+l+";")}}n?t.styles=r:t.stylesWithoutHost=r,n?t.classes=o:t.classesWithoutHost=o}function ry(t,e=0){let n=Re();if(n===null)return F(t,e);let r=tn();return iv(r,n,Xe(t),e)}function ux(t,e,n,r,o){let i=r===null?null:{"":-1},s=o(t,n);if(s!==null){let a=s,c=null,l=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(s);break}px(t,e,n,a,i,c,l)}i!==null&&r!==null&&dx(n,r,i)}function dx(t,e,n){let r=t.localNames=[];for(let o=0;o<e.length;o+=2){let i=n[e[o+1]];if(i==null)throw new x(-301,!1);r.push(e[o],i)}}function fx(t,e,n){e.componentOffset=n,(t.components??=[]).push(e.index)}function px(t,e,n,r,o,i,s){let a=r.length,c=null;for(let h=0;h<a;h++){let p=r[h];c===null&&sr(p)&&(c=p,fx(t,n,h)),LS(tv(n,e),t,p.type)}bx(n,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let h=0;h<a;h++){let p=r[h];p.providersResolver&&p.providersResolver(p)}let l=!1,u=!1,d=Tv(t,e,a,null);a>0&&(n.directiveToIndex=new Map);for(let h=0;h<a;h++){let p=r[h];if(n.mergedAttrs=Gd(n.mergedAttrs,p.hostAttrs),mx(t,n,e,d,p),yx(d,p,o),s!==null&&s.has(p)){let[E,b]=s.get(p);n.directiveToIndex.set(p.type,[d,E+n.directiveStart,b+n.directiveStart])}else(i===null||!i.has(p))&&n.directiveToIndex.set(p.type,d);p.contentQueries!==null&&(n.flags|=4),(p.hostBindings!==null||p.hostAttrs!==null||p.hostVars!==0)&&(n.flags|=64);let g=p.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((t.preOrderHooks??=[]).push(n.index),l=!0),!u&&(g.ngOnChanges||g.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(n.index),u=!0),d++}hx(t,n,i)}function hx(t,e,n){for(let r=e.directiveStart;r<e.directiveEnd;r++){let o=t.data[r];if(n===null||!n.has(o))Pg(0,e,o,r),Pg(1,e,o,r),$g(e,r,!1);else{let i=n.get(o);Lg(0,e,i,r),Lg(1,e,i,r),$g(e,r,!0)}}}function Pg(t,e,n,r){let o=t===0?n.inputs:n.outputs;for(let i in o)if(o.hasOwnProperty(i)){let s;t===0?s=e.inputs??={}:s=e.outputs??={},s[i]??=[],s[i].push(r),oy(e,i)}}function Lg(t,e,n,r){let o=t===0?n.inputs:n.outputs;for(let i in o)if(o.hasOwnProperty(i)){let s=o[i],a;t===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[s]??=[],a[s].push(r,i),oy(e,s)}}function oy(t,e){e==="class"?t.flags|=8:e==="style"&&(t.flags|=16)}function $g(t,e,n){let{attrs:r,inputs:o,hostDirectiveInputs:i}=t;if(r===null||!n&&o===null||n&&i===null||Xd(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<r.length;){let c=r[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!n&&o.hasOwnProperty(c)){let l=o[c];for(let u of l)if(u===e){s??=[],s.push(c,r[a+1]);break}}else if(n&&i.hasOwnProperty(c)){let l=i[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){s??=[],s.push(l[u+1],r[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function mx(t,e,n,r,o){t.data[r]=o;let i=o.factory||(o.factory=Yn(o.type,!0)),s=new Ei(i,sr(o),ry,null);t.blueprint[r]=s,n[r]=s,gx(t,e,r,Tv(t,n,o.hostVars,Dn),o)}function gx(t,e,n,r,o){let i=o.hostBindings;if(i){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~e.index;vx(s)!=a&&s.push(a),s.push(n,r,i)}}function vx(t){let e=t.length;for(;e>0;){let n=t[--e];if(typeof n=="number"&&n<0)return n}return 0}function yx(t,e,n){if(n){if(e.exportAs)for(let r=0;r<e.exportAs.length;r++)n[e.exportAs[r]]=t;sr(e)&&(n[""]=t)}}function bx(t,e,n){t.flags|=1,t.directiveStart=e,t.directiveEnd=e+n,t.providerIndexes=e}function iy(t,e,n,r,o,i,s,a){let c=e[H],l=c.consts,u=pi(l,s),d=sf(c,t,n,r,u);return i&&ux(c,e,d,pi(l,a),o),d.mergedAttrs=Gd(d.mergedAttrs,d.attrs),d.attrs!==null&&Ha(d,d.attrs,!1),d.mergedAttrs!==null&&Ha(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function sy(t,e){CS(t,e),Qu(e)&&t.queries.elementEnd(e)}function _x(t,e,n,r,o,i){let s=e.consts,a=pi(s,o),c=sf(e,t,n,r,a);if(c.mergedAttrs=Gd(c.mergedAttrs,c.attrs),i!=null){let l=pi(s,i);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&Ha(c,c.attrs,!1),c.mergedAttrs!==null&&Ha(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function af(t,e,n){if(n===Dn)return!1;let r=t[e];return Object.is(r,n)?!1:(t[e]=n,!0)}function ay(t){return t.debugInfo?.className||t.type.name||null}function Ex(t){return Object.keys(t).map(e=>{let[n,r,o]=t[e],i={propName:n,templateName:e,isSignal:(r&ec.SignalBased)!==0};return o&&(i.transform=o),i})}function wx(t){return Object.keys(t).map(e=>({propName:t[e],templateName:e}))}function Ix(t,e,n){let r=e instanceof ye?e:e?.injector;return r&&t.getStandaloneInjector!==null&&(r=t.getStandaloneInjector(r)||r),r?new Ud(n,r):n}function Sx(t){let e=t.get(dr,null);if(e===null)throw new x(407,!1);let n=t.get(ny,null),r=t.get(Kr,null),o=t.get(lo,null,{optional:!0});return{rendererFactory:e,sanitizer:n,changeDetectionScheduler:r,ngReflect:!1,tracingService:o}}function Cx(t,e){let n=cy(t);return _v(e,n,n==="svg"?Gm:n==="math"?Km:null)}function xx(t){if(t?.toLowerCase()==="script")throw new x(905,!1)}function cy(t){return(t.selectors[0][0]||"div").toLowerCase()}function Dx(t,e,n,r){let o=t?["ng-version","21.2.18"]:uC(e.selectors[0]),i=null,s=null,a=0;if(n)for(let u of n)a+=u[Hd].requiredVars,u.create&&(u.targetIdx=0,(i??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(r)for(let u=0;u<r.length;u++){let d=r[u];if(typeof d!="function")for(let h of d.bindings){a+=h[Hd].requiredVars;let p=u+1;h.create&&(h.targetIdx=p,(i??=[]).push(h)),h.update&&(h.targetIdx=p,(s??=[]).push(h))}}let c=[e];if(r)for(let u of r){let d=typeof u=="function"?u:u.type,h=Fu(d);c.push(h)}return Cv(0,null,Tx(i,s),1,a,c,null,null,null,[o],null)}function Tx(t,e){return!t&&!e?null:n=>{if(n&1&&t)for(let r of t)r.create();if(n&2&&e)for(let r of e)r.update()}}function Vg(t){let e=t[Hd].kind;return e==="input"||e==="twoWay"}function Mx(t,e,n){let r=t.projection=[];for(let o=0;o<e.length;o++){let i=n[o];r.push(i!=null&&i.length?Array.from(i):null)}}function Nx(){let t=tn();return Rx(t,Re())}function Fg(t){return t[di]}function xd(t){return t[di]||(t[di]=[])}function Rx(t,e){let n,r=e[t.index];return xt(r)?n=r:(n=rx(r,e,null,t),e[t.index]=n,Mv(e,n)),Ax(n,e,t,r),new Bd(n,t,e)}function kx(t,e){let n=t[Ve],r=n.createComment(""),o=Xt(e,t),i=n.parentNode(o);return Ua(n,i,r,n.nextSibling(o),!1),r}function Ox(t,e,n,r){if(t[wn])return;let o;n.type&8?o=Dt(r):o=kx(e,n),t[wn]=o}function Mi(t,e,n=null){return new Si({providers:t,parent:e,debugName:n,runEnvironmentInitializers:!0}).injector}function pr(t){return Za(()=>{let e=ly(t),n=T(m({},e),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===Zd.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&t.dependencies||null,getStandaloneInjector:e.standalone?o=>o.get(Px).getOrCreateStandaloneInjector(n):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Mt.Emulated,styles:t.styles||yn,_:null,schemas:t.schemas||null,tView:null,id:""});e.standalone&&nf("NgStandalone"),uy(n);let r=t.dependencies;return n.directiveDefs=jg(r,Lx),n.pipeDefs=jg(r,Am),n.id=Fx(n),n})}function Lx(t){return _n(t)||Fu(t)}function $x(t,e){if(t==null)return tr;let n={};for(let r in t)if(t.hasOwnProperty(r)){let o=t[r],i,s,a,c;Array.isArray(o)?(a=o[0],i=o[1],s=o[2]??i,c=o[3]||null):(i=o,s=o,a=ec.None,c=null),n[i]=[r,a,c],e[i]=s}return n}function Vx(t){if(t==null)return tr;let e={};for(let n in t)t.hasOwnProperty(n)&&(e[t[n]]=n);return e}function cf(t){return Za(()=>{let e=ly(t);return uy(e),e})}function ly(t){let e={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:e,inputConfig:t.inputs||tr,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||yn,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:$x(t.inputs,e),outputs:Vx(t.outputs),debugInfo:null}}function uy(t){t.features?.forEach(e=>e(t))}function jg(t,e){return t?()=>{let n=typeof t=="function"?t():t,r=[];for(let o of n){let i=e(o);i!==null&&r.push(i)}return r}:null}function Fx(t){let e=0,n=typeof t.consts=="function"?"":t.consts,r=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,n,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let i of r.join("|"))e=Math.imul(31,e)+i.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function Ni(t){return!!t&&typeof t.then=="function"}function dy(t){return!!t&&typeof t.subscribe=="function"}function ic(t){return Jt([{provide:df,multi:!0,useValue:t}])}function fy(){kl(()=>{let t="";throw new x(600,t)})}function py(t){return t.isBoundToModule}function _i(t,e){let n=t.indexOf(e);n>-1&&t.splice(n,1)}function Ug(t,e,n,r,o){Hv(e,t,n,o?"class":"style",r)}function ct(t,e,n,r){let o=Re(),i=o[H],s=t+bt,a=i.firstCreatePass?iy(s,o,2,e,jC,Xm(),n,r):i.data[s];if(ir(a)){let c=o[jt].tracingService;if(c&&c.componentCreate){let l=i.data[a.directiveStart+a.componentOffset];return c.componentCreate(ay(l),()=>(Hg(t,e,o,a,r),ct))}}return Hg(t,e,o,a,r),ct}function Hg(t,e,n,r,o){if(jv(r,n,t,e,hy),Zu(r)){let i=n[H];Vv(i,n,r),yv(i,r,n)}o!=null&&Fv(n,r)}function Nt(){let t=to(),e=tn(),n=Uv(e);return t.firstCreatePass&&sy(t,n),rd(n)&&od(),nd(),n.classesWithoutHost!=null&&DS(n)&&Ug(t,n,Re(),n.classesWithoutHost,!0),n.stylesWithoutHost!=null&&TS(n)&&Ug(t,n,Re(),n.stylesWithoutHost,!1),Nt}function hr(t,e,n,r){return ct(t,e,n,r),Nt(),hr}function Ri(t,e,n,r){let o=Re(),i=o[H],s=t+bt,a=i.firstCreatePass?_x(s,i,2,e,n,r):i.data[s];return jv(a,o,t,e,hy),r!=null&&Fv(o,a),Ri}function ki(){let t=tn(),e=Uv(t);return rd(e)&&od(),nd(),ki}function ac(t,e,n,r){return Ri(t,e,n,r),ki(),ac}function cc(t,e,n){let r=Re(),o=ld();if(af(r,o,e)){let i=to(),s=fg();LC(s,r,t,e,r[Ve],n)}return cc}function my(t){typeof t=="string"&&(Ux=t.toLowerCase().replace(/_/g,"-"))}function Oa(t,e){return t<<17|e<<2}function fr(t){return t>>17&32767}function Hx(t){return(t&2)==2}function Bx(t,e){return t&131071|e<<17}function zd(t){return t|2}function ao(t){return(t&131068)>>2}function Dd(t,e){return t&-131069|e<<2}function zx(t){return(t&1)===1}function qd(t){return t|1}function qx(t,e,n,r,o,i){let s=i?e.classBindings:e.styleBindings,a=fr(s),c=ao(s);t[r]=n;let l=!1,u;if(Array.isArray(n)){let d=n;u=d[1],(u===null||Qr(d,u)>0)&&(l=!0)}else u=n;if(o)if(c!==0){let h=fr(t[a+1]);t[r+1]=Oa(h,a),h!==0&&(t[h+1]=Dd(t[h+1],r)),t[a+1]=Bx(t[a+1],r)}else t[r+1]=Oa(a,0),a!==0&&(t[a+1]=Dd(t[a+1],r)),a=r;else t[r+1]=Oa(c,0),a===0?a=r:t[c+1]=Dd(t[c+1],r),c=r;l&&(t[r+1]=zd(t[r+1])),Bg(t,u,r,!0),Bg(t,u,r,!1),Wx(e,u,t,r,i),s=Oa(a,c),i?e.classBindings=s:e.styleBindings=s}function Wx(t,e,n,r,o){let i=o?t.residualClasses:t.residualStyles;i!=null&&typeof e=="string"&&Qr(i,e)>=0&&(n[r+1]=qd(n[r+1]))}function Bg(t,e,n,r){let o=t[n+1],i=e===null,s=r?fr(o):ao(o),a=!1;for(;s!==0&&(a===!1||i);){let c=t[s],l=t[s+1];Gx(c,e)&&(a=!0,t[s+1]=r?qd(l):zd(l)),s=r?fr(l):ao(l)}a&&(t[n+1]=r?zd(o):qd(o))}function Gx(t,e){return t===null||e==null||(Array.isArray(t)?t[1]:t)===e?!0:Array.isArray(t)&&typeof e=="string"?Qr(t,e)>=0:!1}function lc(t,e){return Kx(t,e,null,!0),lc}function Kx(t,e,n,r){let o=Re(),i=to(),s=og(2);if(i.firstUpdatePass&&Zx(i,t,s,r),e!==Dn&&af(o,s,e)){let a=i.data[cr()];tD(i,a,o,o[Ve],t,o[s+1]=nD(e,n),r,s)}}function Qx(t,e){return e>=t.expandoStartIndex}function Zx(t,e,n,r){let o=t.data;if(o[n+1]===null){let i=o[cr()],s=Qx(t,n);rD(i,r)&&e===null&&!s&&(e=!1),e=Yx(o,i,e,r),qx(o,i,e,n,s,r)}}function Yx(t,e,n,r){let o=cg(t),i=r?e.residualClasses:e.residualStyles;if(o===null)(r?e.classBindings:e.styleBindings)===0&&(n=Td(null,t,e,n,r),n=Ci(n,e.attrs,r),i=null);else{let s=e.directiveStylingLast;if(s===-1||t[s]!==o)if(n=Td(o,t,e,n,r),i===null){let c=Jx(t,e,r);c!==void 0&&Array.isArray(c)&&(c=Td(null,t,e,c[1],r),c=Ci(c,e.attrs,r),Xx(t,e,r,c))}else i=eD(t,e,r)}return i!==void 0&&(r?e.residualClasses=i:e.residualStyles=i),n}function Jx(t,e,n){let r=n?e.classBindings:e.styleBindings;if(ao(r)!==0)return t[fr(r)]}function Xx(t,e,n,r){let o=n?e.classBindings:e.styleBindings;t[fr(o)]=r}function eD(t,e,n){let r,o=e.directiveEnd;for(let i=1+e.directiveStylingLast;i<o;i++){let s=t[i].hostAttrs;r=Ci(r,s,n)}return Ci(r,e.attrs,n)}function Td(t,e,n,r,o){let i=null,s=n.directiveEnd,a=n.directiveStylingLast;for(a===-1?a=n.directiveStart:a++;a<s&&(i=e[a],r=Ci(r,i.hostAttrs,o),i!==t);)a++;return t!==null&&(n.directiveStylingLast=a),r}function Ci(t,e,n){let r=n?1:2,o=-1;if(e!==null)for(let i=0;i<e.length;i++){let s=e[i];typeof s=="number"?o=s:o===r&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Fm(t,s,n?!0:e[++i]))}return t===void 0?null:t}function tD(t,e,n,r,o,i,s,a){if(!(e.type&3))return;let c=t.data,l=c[a+1],u=zx(l)?zg(c,e,n,o,ao(l),s):void 0;if(!Ga(u)){Ga(i)||Hx(l)&&(i=zg(c,null,n,o,a,s));let d=Yu(cr(),n);kC(r,s,d,o,i)}}function zg(t,e,n,r,o,i){let s=e===null,a;for(;o>0;){let c=t[o],l=Array.isArray(c),u=l?c[1]:c,d=u===null,h=n[o+1];h===Dn&&(h=d?yn:void 0);let p=d?Ia(h,r):u===r?h:void 0;if(l&&!Ga(p)&&(p=Ia(c,r)),Ga(p)&&(a=p,s))return a;let g=t[o+1];o=s?fr(g):ao(g)}if(e!==null){let c=i?e.residualClasses:e.residualStyles;c!=null&&(a=Ia(c,r))}return a}function Ga(t){return t!==void 0}function nD(t,e){return t==null||t===""||(typeof e=="string"?t=t+e:typeof t=="object"&&(t=ri(bv(t)))),t}function rD(t,e){return(t.flags&(e?8:16))!==0}function Tn(t,e=""){let n=Re(),r=to(),o=t+bt,i=r.firstCreatePass?sf(r,o,1,e,null):r.data[o],s=oD(r,n,i,e);n[o]=s,hd()&&Pv(r,n,s,i),gi(i,!1)}function iD(t,e,n,r=""){return af(t,ld(),n)?e+Om(n)+r:Dn}function uc(t,e,n){let r=Re(),o=iD(r,t,e,n);return o!==Dn&&sD(r,cr(),o),uc}function sD(t,e,n){let r=Yu(e,t);ZS(t[Ve],r,n)}function vy(){return[{provide:Kr,useExisting:gy},{provide:Qe,useClass:ni},{provide:vi,useValue:!0}]}function aD(){return typeof $localize<"u"&&$localize.locale||Ai}var Va,Wd,Qg,_g,ce,re,ro,Ei,Md,AS,Xg,ev,OS,Ut,lr,cv,Zd,uv,BS,xg,WS,Ya,GS,Ja,Di,Xa,hv,mv,KS,Mt,Od,Sv,Dn,ec,rn,hC,ZL,Ld,yi,oo,tf,lo,Mg,Rv,vC,MC,kg,PC,Wv,KC,ZC,YC,ur,lx,ty,nc,jd,Ti,dr,ny,$a,Ud,Hd,Ba,io,za,rc,Bd,Ax,so,oc,qa,Wa,Si,Px,lf,uf,df,ff,sc,jx,on,hy,Ai,Ux,oD,Ka,pf,gy,hf,mf=f(()=>{"use strict";ka();Hn();Kn();Aa();Va=class{previousValue;currentValue;firstChange;constructor(e,n,r){this.previousValue=e,this.currentValue=n,this.firstChange=r}isFirstChange(){return this.firstChange}},Wd=(()=>{let t=()=>Kg;return t.ngInherit=!0,t})();Qg="__ngSimpleChanges__";_g=[],ce=function(t,e=null,n){for(let r=0;r<_g.length;r++){let o=_g[r];o(t,e,n)}},re=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(re||{});ro=-1,Ei=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,n,r,o){this.factory=e,this.name=o,this.canSeeViewProviders=n,this.injectImpl=r}};Md=!0;AS=256,Xg=AS-1,ev=5,OS=0,Ut={};lr=class{_tNode;_lView;constructor(e,n){this._tNode=e,this._lView=n}get(e,n,r){return iv(this._tNode,this._lView,e,Zn(r),n)}};cv=(()=>{class t{nativeElement;constructor(n){this.nativeElement=n}static __NG_ELEMENT_ID__=HS}return t})();Zd=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Zd||{}),uv=new Map,BS=0;xg="__ngContext__";Ya=new k("",{factory:()=>GS}),GS="ng",Ja=new k(""),Di=new k("",{providedIn:"platform",factory:()=>"unknown"}),Xa=new k("",{factory:()=>y(be).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null}),hv=!1,mv=new k("",{factory:()=>hv});KS=()=>null;Mt=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Mt||{}),Od=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Mu})`}};Sv="ng-template";Dn={};ec=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(ec||{});rn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(rn||{});ZL=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function",Ld=new WeakMap,yi=new WeakSet;oo=new Set,tf=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(tf||{}),lo=new k(""),Mg=new Set;Rv=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=A({token:t,providedIn:"root",factory:()=>new t})}return t})(),vC=new k("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:y(ye)})});MC=TC;PC=()=>null;Wv=[];KC=T(m({},Ar),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{mi(t.lView)},consumerOnSignalRead(){this.lView[at]=this}});ZC=T(m({},Ar),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let e=bn(t.lView);for(;e&&!Gv(e[H]);)e=bn(e);e&&ed(e)},consumerOnSignalRead(){this.lView[at]=this}});YC=100;ur=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,n=e[H];return Ii(n,e,n.firstChild,[])}constructor(e,n){this._lView=e,this._cdRefInjectingView=n}get context(){return this._lView[st]}set context(e){this._lView[st]=e}get destroyed(){return ar(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Ne];if(xt(e)){let n=e[di],r=n?n.indexOf(this):-1;r>-1&&(Fd(e,r),ii(n,r))}this._attachedToViewContainer=!1}Ov(this._lView[H],this._lView)}onDestroy(e){td(this._lView,e)}markForCheck(){Xv(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[U]&=-129}reattach(){Ta(this._lView),this._lView[U]|=128}detectChanges(){this._lView[U]|=1024,Qv(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new x(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=eo(this._lView),n=this._lView[Jr];n!==null&&!e&&rf(n,this._lView),Av(this._lView[H],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new x(902,!1);this._appRef=e;let n=eo(this._lView),r=this._lView[Jr];r!==null&&!n&&ey(r,this._lView),Ta(this._lView)}};lx=()=>null;ty=class{},nc=class{},jd=class{resolveComponentFactory(e){throw new x(917,!1)}},Ti=class{static NULL=new jd},dr=class{},ny=(()=>{class t{static \u0275prov=A({token:t,providedIn:"root",factory:()=>null})}return t})(),$a={},Ud=class{injector;parentInjector;constructor(e,n){this.injector=e,this.parentInjector=n}get(e,n,r){let o=this.injector.get(e,$a,r);return o!==$a||n===$a?o:this.parentInjector.get(e,n,r)}};Hd=Symbol("BINDING");Ba=class extends Ti{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let n=_n(e);return new io(n,this.ngModule)}};io=class extends nc{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=Ex(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=wx(this.componentDef.outputs),this.cachedOutputs}constructor(e,n){super(),this.componentDef=e,this.ngModule=n,this.componentType=e.type,this.selector=lC(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!n}create(e,n,r,o,i,s){ce(re.DynamicComponentStart);let a=B(null);try{let c=this.componentDef,l=Ix(c,o||this.ngModule,e),u=Sx(l),d=u.tracingService;return d&&d.componentCreate?d.componentCreate(ay(c),()=>this.createComponentRef(u,l,n,r,i,s)):this.createComponentRef(u,l,n,r,i,s)}finally{B(a)}}createComponentRef(e,n,r,o,i,s){let a=this.componentDef,c=Dx(o,a,s,i),l=e.rendererFactory.createRenderer(null,a),u=o?AC(l,o,a.encapsulation,n):Cx(a,l);xx(u?.tagName);let d=s?.some(Vg)||i?.some(g=>typeof g!="function"&&g.bindings.some(Vg)),h=xv(null,c,null,512|Dv(a),null,null,e,l,n,null,gv(u,n,!0));h[bt]=u,Na(h);let p=null;try{let g=iy(bt,h,2,"#host",()=>c.directiveRegistry,!0,0);wv(l,u,g),wi(u,h),Vv(c,h,g),yv(c,g,h),sy(c,g),r!==void 0&&Mx(g,this.ngContentSelectors,r),p=en(g.index,h),h[st]=p[st],Bv(c,h,null)}catch(g){throw p!==null&&kd(p),kd(h),g}finally{ce(re.DynamicComponentEnd),Ra()}return new za(this.componentType,h,!!d)}};za=class extends ty{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,n,r){super(),this._rootLView=n,this._hasInputBindings=r,this._tNode=Ju(n[H],bt),this.location=Qd(this._tNode,n),this.instance=en(this._tNode.index,n)[st],this.hostView=this.changeDetectorRef=new ur(n,void 0),this.componentType=e}setInput(e,n){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),n))return;let o=this._rootLView,i=Hv(r,o[H],o,e,n);this.previousInputValues.set(e,n);let s=en(r.index,o);Xv(s,1)}get injector(){return new lr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};rc=(()=>{class t{static __NG_ELEMENT_ID__=Nx}return t})();Bd=class t extends rc{_lContainer;_hostTNode;_hostLView;constructor(e,n,r){super(),this._lContainer=e,this._hostTNode=n,this._hostLView=r}get element(){return Qd(this._hostTNode,this._hostLView)}get injector(){return new lr(this._hostTNode,this._hostLView)}get parentInjector(){let e=Kd(this._hostTNode,this._hostLView);if(Jg(e)){let n=ja(e,this._hostLView),r=Fa(e),o=n[H].data[r+8];return new lr(o,n)}else return new lr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let n=Fg(this._lContainer);return n!==null&&n[e]||null}get length(){return this._lContainer.length-et}createEmbeddedView(e,n,r){let o,i;typeof r=="number"?o=r:r!=null&&(o=r.index,i=r.injector);let s=Og(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(n||{},i,s);return this.insertImpl(a,o,Ag(this._hostTNode,s)),a}createComponent(e,n,r,o,i,s,a){let c=e&&!_S(e),l;if(c)l=n;else{let b=n||{};l=b.index,r=b.injector,o=b.projectableNodes,i=b.environmentInjector||b.ngModuleRef,s=b.directives,a=b.bindings}let u=c?e:new io(_n(e)),d=r||this.parentInjector;if(!i&&u.ngModule==null){let D=(c?d:this.parentInjector).get(ye,null);D&&(i=D)}let h=_n(u.componentType??{}),p=Og(this._lContainer,h?.id??null),g=p?.firstChild??null,E=u.create(d,o,g,i,s,a);return this.insertImpl(E.hostView,l,Ag(this._hostTNode,p)),E}insert(e,n){return this.insertImpl(e,n,!0)}insertImpl(e,n,r){let o=e._lView;if(Qm(o)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=o[Ne],l=new t(c,c[vt],c[Ne]);l.detach(l.indexOf(e))}}let i=this._adjustIndex(n),s=this._lContainer;return ox(s,o,i,r),e.attachToViewContainerRef(),Hu(xd(s),i,e),e}move(e,n){return this.insert(e,n)}indexOf(e){let n=Fg(this._lContainer);return n!==null?n.indexOf(e):-1}remove(e){let n=this._adjustIndex(e,-1),r=Fd(this._lContainer,n);r&&(ii(xd(this._lContainer),n),Ov(r[H],r))}detach(e){let n=this._adjustIndex(e,-1),r=Fd(this._lContainer,n);return r&&ii(xd(this._lContainer),n)!=null?new ur(r):null}_adjustIndex(e,n=0){return e??this.length+n}};Ax=Ox;so=class{},oc=class{},qa=class extends so{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Ba(this);constructor(e,n,r,o=!0){super(),this.ngModuleType=e,this._parent=n;let i=Vu(e);this._bootstrapComponents=Iv(i.bootstrap),this._r3Injector=gd(e,n,[{provide:so,useValue:this},{provide:Ti,useValue:this.componentFactoryResolver},...r],ri(e),new Set(["environment"])),o&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(n=>n()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},Wa=class extends oc{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new qa(this.moduleType,e,[])}},Si=class extends so{injector;componentFactoryResolver=new Ba(this);instance=null;constructor(e){super();let n=new Jn([...e.providers,{provide:so,useValue:this},{provide:Ti,useValue:this.componentFactoryResolver}],e.parent||ai(),e.debugName,new Set(["environment"]));this.injector=n,e.runEnvironmentInitializers&&n.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};Px=(()=>{class t{_injector;cachedInjectors=new Map;constructor(n){this._injector=n}getOrCreateStandaloneInjector(n){if(!n.standalone)return null;if(!this.cachedInjectors.has(n)){let r=qu(!1,n.type),o=r.length>0?Mi([r],this._injector,""):null;this.cachedInjectors.set(n,o)}return this.cachedInjectors.get(n)}ngOnDestroy(){try{for(let n of this.cachedInjectors.values())n!==null&&n.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=A({token:t,providedIn:"environment",factory:()=>new t(F(ye))})}return t})();lf=(()=>{class t{log(n){console.log(n)}warn(n){console.warn(n)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})(),uf=new k("");df=new k("");ff=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((n,r)=>{this.resolve=n,this.reject=r});appInits=y(df,{optional:!0})??[];injector=y(mt);constructor(){}runInitializers(){if(this.initialized)return;let n=[];for(let o of this.appInits){let i=$e(this.injector,o);if(Ni(i))n.push(i);else if(dy(i)){let s=new Promise((a,c)=>{i.subscribe({complete:a,error:c})});n.push(s)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(n).then(()=>{r()}).catch(o=>{this.reject(o)}),n.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),sc=new k("");jx=10,on=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=y(nn);afterRenderManager=y(Rv);zonelessEnabled=y(vi);rootEffectScheduler=y(wd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new me;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=y(xn);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ne(n=>!n))}constructor(){y(lo,{optional:!0})}whenStable(){let n;return new Promise(r=>{n=this.isStable.subscribe({next:o=>{o&&r()}})}).finally(()=>{n.unsubscribe()})}_injector=y(ye);_rendererFactory=null;get injector(){return this._injector}bootstrap(n,r){return this.bootstrapImpl(n,r)}bootstrapImpl(n,r,o=mt.NULL){return this._injector.get(Qe).run(()=>{ce(re.BootstrapComponentStart);let s=n instanceof nc;if(!this._injector.get(ff).done){let g="";throw new x(405,g)}let c;s?c=n:c=this._injector.get(Ti).resolveComponentFactory(n),this.componentTypes.push(c.componentType);let l=py(c)?void 0:this._injector.get(so),u=r||c.selector,d=c.create(o,[],u,l),h=d.location.nativeElement,p=d.injector.get(uf,null);return p?.registerApplication(h),d.onDestroy(()=>{this.detachView(d.hostView),_i(this.components,d),p?.unregisterApplication(h)}),this._loadComponent(d),ce(re.BootstrapComponentEnd,d),d})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ce(re.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(tf.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ce(re.ChangeDetectionEnd),new x(101,!1);let n=B(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,B(n),this.afterTick.next(),ce(re.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(dr,null,{optional:!0}));let n=0;for(;this.dirtyFlags!==0&&n++<jx;){ce(re.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ce(re.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let n=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:o}of this.allViews){if(!r&&!hi(o))continue;let i=r&&!this.zonelessEnabled?0:1;Qv(o,i),n=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}n||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:n})=>hi(n))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(n){let r=n;this._views.push(r),r.attachToAppRef(this)}detachView(n){let r=n;_i(this._views,r),r.detachFromAppRef()}_loadComponent(n){this.attachView(n.hostView);try{this.tick()}catch(o){this.internalErrorHandler(o)}this.components.push(n),this._injector.get(sc,[]).forEach(o=>o(n))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(n=>n()),this._views.slice().forEach(n=>n.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(n){return this._destroyListeners.push(n),()=>_i(this._destroyListeners,n)}destroy(){if(this._destroyed)throw new x(406,!1);let n=this._injector;n.destroy&&!n.destroyed&&n.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();hy=(t,e,n,r,o)=>(md(!0),_v(e[Ve],r,pd()));Ai="en-US",Ux=Ai;oD=(t,e,n,r)=>(md(!0),QS(e[Ve],r));Ka=class{ngModuleFactory;componentFactories;constructor(e,n){this.ngModuleFactory=e,this.componentFactories=n}},pf=(()=>{class t{compileModuleSync(n){return new Wa(n)}compileModuleAsync(n){return Promise.resolve(this.compileModuleSync(n))}compileModuleAndAllComponentsSync(n){let r=this.compileModuleSync(n),o=Vu(n),i=Iv(o.declarations).reduce((s,a)=>{let c=_n(a);return c&&s.push(new io(c)),s},[]);return new Ka(r,i)}compileModuleAndAllComponentsAsync(n){return Promise.resolve(this.compileModuleAndAllComponentsSync(n))}clearCache(){}clearCacheFor(n){}getModuleId(n){}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),gy=(()=>{class t{applicationErrorHandler=y(nn);appRef=y(on);taskService=y(xn);ngZone=y(Qe);zonelessEnabled=y(vi);tracing=y(lo,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new De;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(ti):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(y(Ed,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let n=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(n);return}this.switchToMicrotaskScheduler(),this.taskService.remove(n)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let n=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(n)})})}notify(n){if(!this.zonelessEnabled&&n===5)return;switch(n){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?gg:vd;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(ti+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let n=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(n),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let n=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(n)}}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();hf=new k("",{factory:()=>y(hf,{optional:!0,skipSelf:!0})||aD()})});function Mn(t){return Cm(t)}function uo(t,e){return Ps(t,e?.equal)}var gf=f(()=>{"use strict";Hn();xm();});function wy(t,e){let n=Object.create(vD);n.value=t,n.transformFn=e?.transform;function r(){if(Or(n),n.value===Ey){let o=null;throw new x(-950,o)}return n.value}return r[qe]=n,r}function yy(t,e){return wy(t,e)}function yD(t){return wy(Ey,t)}function Oi(t){return!t.moduleRef}function _D(t){let e=Oi(t)?t.r3Injector:t.moduleRef.injector,n=e.get(Qe);return n.run(()=>{Oi(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let r=e.get(nn),o;if(n.runOutsideAngular(()=>{o=n.onError.subscribe({next:r})}),Oi(t)){let i=()=>e.destroy(),s=t.platformInjector.get(vf);s.add(i),e.onDestroy(()=>{o.unsubscribe(),s.delete(i)})}else{let i=()=>t.moduleRef.destroy(),s=t.platformInjector.get(vf);s.add(i),t.moduleRef.onDestroy(()=>{_i(t.allPlatformModules,t.moduleRef),o.unsubscribe(),s.delete(i)})}return wD(r,n,()=>{let i=e.get(xn),s=i.add(),a=e.get(ff);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(hf,Ai);if(my(c||Ai),!e.get(bD,!0))return Oi(t)?e.get(on):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Oi(t)){let u=e.get(on);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return ED?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{i.remove(s)})})})}function wD(t,e,n){try{let r=n();return Ni(r)?r.catch(o=>{throw e.runOutsideAngular(()=>t(o)),o}):r}catch(r){throw e.runOutsideAngular(()=>t(r)),r}}function ID(t=[],e){return mt.create({name:e,providers:[{provide:si,useValue:"platform"},{provide:vf,useValue:new Set([()=>dc=null])},...t]})}function SD(t=[]){if(dc)return dc;let e=ID(t);return dc=e,fy(),CD(e),e}function CD(t){let e=t.get(Ja,null);$e(t,()=>{e?.forEach(n=>n())})}function Sy(){return!1}function DD(t){return TD(tn(),Re(),(t&16)===16)}function TD(t,e,n){if(ir(t)&&!n){let r=en(t.index,e);return new ur(r,r)}else if(t.type&175){let r=e[yt];return new ur(r,e)}return null}function xy(t){let{rootComponent:e,appProviders:n,platformProviders:r,platformRef:o}=t;ce(re.BootstrapApplicationStart);try{let i=o?.injector??SD(r),s=[vy(),yg,...n||[]],a=new Si({providers:s,parent:i,debugName:"",runEnvironmentInitializers:!1});return _D({r3Injector:a.injector,platformInjector:i,rootComponent:e})}catch(i){return Promise.reject(i)}finally{ce(re.BootstrapApplicationEnd)}}var Ey,vD,Iy,vf,bD,ED,dc,xD,vB,Cy,fe=f(()=>{"use strict";mf();mf();ka();ka();gf();gf();Hn();Ey=Symbol("InputSignalNode#UNSET"),vD=T(m({},Ls),{transformFn:void 0,applyValueToInputSignal(t,e){$r(t,e)}});Iy=(yy.required=yD,yy),vf=new k(""),bD=new k("");dc=null;xD=1e4,vB=xD-1e3,Cy=(()=>{class t{static __NG_ELEMENT_ID__=DD}return t})()});function sn(){return Dy}function bf(t){Dy??=t}var Dy,Pi,fc,Ty,_f=f(()=>{"use strict";fe();fe();Dy=null;Pi=class{},fc=(()=>{class t{historyGo(n){throw new Error("")}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>y(Ty),providedIn:"platform"})}return t})(),Ty=(()=>{class t extends fc{_location;_history;_doc=y(be);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return sn().getBaseHref(this._doc)}onPopState(n){let r=sn().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",n,!1),()=>r.removeEventListener("popstate",n)}onHashChange(n){let r=sn().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",n,!1),()=>r.removeEventListener("hashchange",n)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(n){this._location.pathname=n}pushState(n,r,o){this._history.pushState(n,r,o)}replaceState(n,r,o){this._history.replaceState(n,r,o)}forward(){this._history.forward()}back(){this._history.back()}historyGo(n=0){this._history.go(n)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>new t,providedIn:"platform"})}return t})()});function Ry(t,e){return t?e?t.endsWith("/")?e.startsWith("/")?t+e.slice(1):t+e:e.startsWith("/")?t+e:`${t}/${e}`:t:e}function My(t){let e=t.search(/#|\?|$/);return t[e-1]==="/"?t.slice(0,e-1)+t.slice(e):t}function Nn(t){return t&&t[0]!=="?"?`?${t}`:t}function RD(){return new fo(F(pc))}function kD(t,e){if(!t||!e.startsWith(t))return e;let n=e.substring(t.length);return n===""||["/",";","?","#"].includes(n[0])?n:e}function Ny(t){return t.replace(/\/index\.html$/,"")}function AD(t){if(new RegExp("^(https?:)?//").test(t)){let[,n]=t.split(/\/\/[^\/]+/);return n}return t}var pc,MD,ND,fo,ky=f(()=>{"use strict";fe();fe();Kn();_f();pc=(()=>{class t{historyGo(n){throw new Error("")}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>y(ND),providedIn:"root"})}return t})(),MD=new k(""),ND=(()=>{class t extends pc{_platformLocation;_baseHref;_removeListenerFns=[];constructor(n,r){super(),this._platformLocation=n,this._baseHref=r??this._platformLocation.getBaseHrefFromDOM()??y(be).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(n){this._removeListenerFns.push(this._platformLocation.onPopState(n),this._platformLocation.onHashChange(n))}getBaseHref(){return this._baseHref}prepareExternalUrl(n){return Ry(this._baseHref,n)}path(n=!1){let r=this._platformLocation.pathname+Nn(this._platformLocation.search),o=this._platformLocation.hash;return o&&n?`${r}${o}`:r}pushState(n,r,o,i){let s=this.prepareExternalUrl(o+Nn(i));this._platformLocation.pushState(n,r,s)}replaceState(n,r,o,i){let s=this.prepareExternalUrl(o+Nn(i));this._platformLocation.replaceState(n,r,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(n=0){this._platformLocation.historyGo?.(n)}static \u0275fac=function(r){return new(r||t)(F(fc),F(MD,8))};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),fo=(()=>{class t{_subject=new me;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(n){this._locationStrategy=n;let r=this._locationStrategy.getBaseHref();this._basePath=AD(My(Ny(r))),this._locationStrategy.onPopState(o=>{this._subject.next({url:this.path(!0),pop:!0,state:o.state,type:o.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(n=!1){return this.normalize(this._locationStrategy.path(n))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(n,r=""){return this.path()==this.normalize(n+Nn(r))}normalize(n){return t.stripTrailingSlash(kD(this._basePath,Ny(n)))}prepareExternalUrl(n){return n&&n[0]!=="/"&&(n="/"+n),this._locationStrategy.prepareExternalUrl(n)}go(n,r="",o=null){this._locationStrategy.pushState(o,"",n,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(n+Nn(r)),o)}replaceState(n,r="",o=null){this._locationStrategy.replaceState(o,"",n,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(n+Nn(r)),o)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(n=0){this._locationStrategy.historyGo?.(n)}onUrlChange(n){return this._urlChangeListeners.push(n),this._urlChangeSubscription??=this.subscribe(r=>{this._notifyUrlChangeListeners(r.url,r.state)}),()=>{let r=this._urlChangeListeners.indexOf(n);this._urlChangeListeners.splice(r,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(n="",r){this._urlChangeListeners.forEach(o=>o(n,r))}subscribe(n,r,o){return this._subject.subscribe({next:n,error:r??void 0,complete:o??void 0})}static normalizeQueryParams=Nn;static joinWithSlash=Ry;static stripTrailingSlash=My;static \u0275fac=function(r){return new(r||t)(F(pc))};static \u0275prov=A({token:t,factory:()=>RD(),providedIn:"root"})}return t})()});function Ef(t,e){e=encodeURIComponent(e);for(let n of t.split(";")){let r=n.indexOf("="),[o,i]=r==-1?[n,""]:[n.slice(0,r),n.slice(r+1)];if(o.trim()===e)return decodeURIComponent(i)}return null}var Li,Ay=f(()=>{"use strict";Li=class{}});var Oy,$i=f(()=>{"use strict";ky();_f();fe();Ay();Oy="browser"});function Py(t){for(let e of t)e.remove()}function Ly(t,e){let n=e.createElement("style");return n.textContent=t,n}function OD(t,e,n,r){let o=t.head?.querySelectorAll(`style[${wf}="${e}"],link[${wf}="${e}"]`);if(o)for(let i of o)i.removeAttribute(wf),i instanceof HTMLLinkElement?r.set(i.href.slice(i.href.lastIndexOf("/")+1),{usage:0,elements:[i]}):i.textContent&&n.set(i.textContent,{usage:0,elements:[i]})}function Sf(t,e){let n=e.createElement("link");return n.setAttribute("rel","stylesheet"),n.setAttribute("href",t),n}function FD(t){return LD.replace(Df,t)}function jD(t){return PD.replace(Df,t)}function Fy(t,e){return e.map(n=>n.replace(Df,t))}function $y(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Vi,hc,vc,Cf,wf,xf,If,Df,Vy,PD,LD,$D,VD,Tf,Fi,mc,ji,gc,jy=f(()=>{"use strict";$i();fe();fe();Vi=class{_doc;constructor(e){this._doc=e}manager},hc=(()=>{class t extends Vi{constructor(n){super(n)}supports(n){return!0}addEventListener(n,r,o,i){return n.addEventListener(r,o,i),()=>this.removeEventListener(n,r,o,i)}removeEventListener(n,r,o,i){return n.removeEventListener(r,o,i)}static \u0275fac=function(r){return new(r||t)(F(be))};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})(),vc=new k(""),Cf=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(n,r){this._zone=r,n.forEach(s=>{s.manager=this});let o=n.filter(s=>!(s instanceof hc));this._plugins=o.slice().reverse();let i=n.find(s=>s instanceof hc);i&&this._plugins.push(i)}addEventListener(n,r,o,i){return this._findPluginFor(r).addEventListener(n,r,o,i)}getZone(){return this._zone}_findPluginFor(n){let r=this._eventNameToPlugin.get(n);if(r)return r;if(r=this._plugins.find(i=>i.supports(n)),!r)throw new x(5101,!1);return this._eventNameToPlugin.set(n,r),r}static \u0275fac=function(r){return new(r||t)(F(vc),F(Qe))};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})(),wf="ng-app-id";xf=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(n,r,o,i={}){this.doc=n,this.appId=r,this.nonce=o,OD(n,r,this.inline,this.external),this.hosts.add(n.head)}addStyles(n,r){for(let o of n)this.addUsage(o,this.inline,Ly);r?.forEach(o=>this.addUsage(o,this.external,Sf))}removeStyles(n,r){for(let o of n)this.removeUsage(o,this.inline);r?.forEach(o=>this.removeUsage(o,this.external))}addUsage(n,r,o){let i=r.get(n);i?i.usage++:r.set(n,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,o(n,this.doc)))})}removeUsage(n,r){let o=r.get(n);o&&(o.usage--,o.usage<=0&&(Py(o.elements),r.delete(n)))}ngOnDestroy(){for(let[,{elements:n}]of[...this.inline,...this.external])Py(n);this.hosts.clear()}addHost(n){this.hosts.add(n);for(let[r,{elements:o}]of this.inline)o.push(this.addElement(n,Ly(r,this.doc)));for(let[r,{elements:o}]of this.external)o.push(this.addElement(n,Sf(r,this.doc)))}removeHost(n){this.hosts.delete(n)}addElement(n,r){return this.nonce&&r.setAttribute("nonce",this.nonce),n.appendChild(r)}static \u0275fac=function(r){return new(r||t)(F(be),F(Ya),F(Xa,8),F(Di))};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})(),If={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Df=/%COMP%/g,Vy="%COMP%",PD=`_nghost-${Vy}`,LD=`_ngcontent-${Vy}`,$D=!0,VD=new k("",{factory:()=>$D});Tf=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(n,r,o,i,s,a,c=null,l=null){this.eventManager=n,this.sharedStylesHost=r,this.appId=o,this.removeStylesOnCompDestroy=i,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Fi(n,s,a,this.tracingService)}createRenderer(n,r){if(!n||!r)return this.defaultRenderer;let o=this.getOrCreateRenderer(n,r);return o instanceof gc?o.applyToHost(n):o instanceof ji&&o.applyStyles(),o}getOrCreateRenderer(n,r){let o=this.rendererByCompId,i=o.get(r.id);if(!i){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(r.encapsulation){case Mt.Emulated:i=new gc(c,l,r,this.appId,u,s,a,d);break;case Mt.ShadowDom:return new mc(c,n,r,s,a,this.nonce,d,l);case Mt.ExperimentalIsolatedShadowDom:return new mc(c,n,r,s,a,this.nonce,d);default:i=new ji(c,l,r,u,s,a,d);break}o.set(r.id,i)}return i}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(n){this.rendererByCompId.delete(n)}static \u0275fac=function(r){return new(r||t)(F(Cf),F(xf),F(Ya),F(VD),F(be),F(Qe),F(Xa),F(lo,8))};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})(),Fi=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,n,r,o){this.eventManager=e,this.doc=n,this.ngZone=r,this.tracingService=o}destroy(){}destroyNode=null;createElement(e,n){return n?this.doc.createElementNS(If[n]||n,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,n){($y(e)?e.content:e).appendChild(n)}insertBefore(e,n,r){e&&($y(e)?e.content:e).insertBefore(n,r)}removeChild(e,n){n.remove()}selectRootElement(e,n){let r=typeof e=="string"?this.doc.querySelector(e):e;if(!r)throw new x(-5104,!1);return n||(r.textContent=""),r}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,n,r,o){if(o){n=o+":"+n;let i=If[o];i?e.setAttributeNS(i,n,r):e.setAttribute(n,r)}else e.setAttribute(n,r)}removeAttribute(e,n,r){if(r){let o=If[r];o?e.removeAttributeNS(o,n):e.removeAttribute(`${r}:${n}`)}else e.removeAttribute(n)}addClass(e,n){e.classList.add(n)}removeClass(e,n){e.classList.remove(n)}setStyle(e,n,r,o){o&(rn.DashCase|rn.Important)?e.style.setProperty(n,r,o&rn.Important?"important":""):e.style[n]=r}removeStyle(e,n,r){r&rn.DashCase?e.style.removeProperty(n):e.style[n]=""}setProperty(e,n,r){e!=null&&(e[n]=r)}setValue(e,n){e.nodeValue=n}listen(e,n,r,o){if(typeof e=="string"&&(e=sn().getGlobalEventTarget(this.doc,e),!e))throw new x(5102,!1);let i=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(i=this.tracingService.wrapEventListener(e,n,i)),this.eventManager.addEventListener(e,n,i,o)}decoratePreventDefault(e){return n=>{if(n==="__ngUnwrap__")return e;e(n)===!1&&n.preventDefault()}}};mc=class extends Fi{hostEl;sharedStylesHost;shadowRoot;constructor(e,n,r,o,i,s,a,c){super(e,o,i,a),this.hostEl=n,this.sharedStylesHost=c,this.shadowRoot=n.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=r.styles;l=Fy(r.id,l);for(let d of l){let h=document.createElement("style");s&&h.setAttribute("nonce",s),h.textContent=d,this.shadowRoot.appendChild(h)}let u=r.getExternalStyles?.();if(u)for(let d of u){let h=Sf(d,o);s&&h.setAttribute("nonce",s),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,n){return super.appendChild(this.nodeOrShadowRoot(e),n)}insertBefore(e,n,r){return super.insertBefore(this.nodeOrShadowRoot(e),n,r)}removeChild(e,n){return super.removeChild(null,n)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ji=class extends Fi{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,n,r,o,i,s,a,c){super(e,i,s,a),this.sharedStylesHost=n,this.removeStylesOnCompDestroy=o;let l=r.styles;this.styles=c?Fy(c,l):l,this.styleUrls=r.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&oo.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},gc=class extends ji{contentAttr;hostAttr;constructor(e,n,r,o,i,s,a,c){let l=o+"-"+r.id;super(e,n,r,i,s,a,c,l),this.contentAttr=FD(l),this.hostAttr=jD(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,n){let r=super.createElement(e,n);return super.setAttribute(r,this.contentAttr,""),r}}});function UD(){return Ui=Ui||document.head.querySelector("base"),Ui?Ui.getAttribute("href"):null}function HD(t){return new URL(t,document.baseURI).pathname}async function Mf(t,e,n){let r=m({rootComponent:t},WD(e,n));return xy(r)}function WD(t,e){return{platformRef:e?.platformRef,appProviders:[...YD,...t?.providers??[]],platformProviders:ZD}}function GD(){yc.makeCurrent()}function KD(){return new Yt}function QD(){return Yd(document),document}var yc,Ui,BD,Uy,zD,qD,Hy,ZD,YD,By=f(()=>{"use strict";$i();fe();fe();jy();yc=class t extends Pi{supportsDOMEvents=!0;static makeCurrent(){bf(new t)}onAndCancel(e,n,r,o){return e.addEventListener(n,r,o),()=>{e.removeEventListener(n,r,o)}}dispatchEvent(e,n){e.dispatchEvent(n)}remove(e){e.remove()}createElement(e,n){return n=n||this.getDefaultDocument(),n.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,n){return n==="window"?window:n==="document"?e:n==="body"?e.body:null}getBaseHref(e){let n=UD();return n==null?null:HD(n)}resetBaseElement(){Ui=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Ef(document.cookie,e)}},Ui=null;BD=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})(),Uy=["alt","control","meta","shift"],zD={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},qD={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},Hy=(()=>{class t extends Vi{constructor(n){super(n)}supports(n){return t.parseEventName(n)!=null}addEventListener(n,r,o,i){let s=t.parseEventName(r),a=t.eventCallback(s.fullKey,o,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>sn().onAndCancel(n,s.domEventName,a,i))}static parseEventName(n){let r=n.toLowerCase().split("."),o=r.shift();if(r.length===0||!(o==="keydown"||o==="keyup"))return null;let i=t._normalizeKey(r.pop()),s="",a=r.indexOf("code");if(a>-1&&(r.splice(a,1),s="code."),Uy.forEach(l=>{let u=r.indexOf(l);u>-1&&(r.splice(u,1),s+=l+".")}),s+=i,r.length!=0||i.length===0)return null;let c={};return c.domEventName=o,c.fullKey=s,c}static matchEventFullKeyCode(n,r){let o=zD[n.key]||n.key,i="";return r.indexOf("code.")>-1&&(o=n.code,i="code."),o==null||!o?!1:(o=o.toLowerCase(),o===" "?o="space":o==="."&&(o="dot"),Uy.forEach(s=>{if(s!==o){let a=qD[s];a(n)&&(i+=s+".")}}),i+=o,i===r)}static eventCallback(n,r,o){return i=>{t.matchEventFullKeyCode(i,n)&&o.runGuarded(()=>r(i))}}static _normalizeKey(n){return n==="esc"?"escape":n}static \u0275fac=function(r){return new(r||t)(F(be))};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})();ZD=[{provide:Di,useValue:Oy},{provide:Ja,useValue:GD,multi:!0},{provide:be,useFactory:QD}],YD=[{provide:si,useValue:"root"},{provide:Yt,useFactory:KD},{provide:vc,useClass:hc,multi:!0},{provide:vc,useClass:Hy,multi:!0},Tf,xf,Cf,{provide:dr,useExisting:Tf},{provide:Li,useClass:BD},[]]});var zy,Nf=f(()=>{"use strict";By();$i();fe();zy=(()=>{class t{_doc;constructor(n){this._doc=n}getTitle(){return this._doc.title}setTitle(n){this._doc.title=n||""}static \u0275fac=function(r){return new(r||t)(F(be))};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})()});function gr(t){return new Pf(t)}function Rf(t,e,n){for(let r=0;r<t.length;r++){let o=t[r],i=e[r];if(o[0]===":")n[o.substring(1)]=i;else if(o!==i.path)return!1}return!0}function Jy(t,e,n){let r=n.path.split("/"),o=r.indexOf("**");if(o===-1){if(r.length>t.length||n.pathMatch==="full"&&(e.hasChildren()||r.length<t.length))return null;let c={},l=t.slice(0,r.length);return Rf(r,l,c)?{consumed:l,posParams:c}:null}if(o!==r.lastIndexOf("**"))return null;let i=r.slice(0,o),s=r.slice(o+1);if(i.length+s.length>t.length||n.pathMatch==="full"&&e.hasChildren()&&n.path!=="**")return null;let a={};return!Rf(i,t.slice(0,i.length),a)||!Rf(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function Sc(t){return new Promise((e,n)=>{t.pipe(Qt()).subscribe({next:r=>e(r),error:r=>n(r)})})}function XD(t,e){if(t.length!==e.length)return!1;for(let n=0;n<t.length;++n)if(!Bt(t[n],e[n]))return!1;return!0}function Bt(t,e){let n=t?Lf(t):void 0,r=e?Lf(e):void 0;if(!n||!r||n.length!=r.length)return!1;let o;for(let i=0;i<n.length;i++)if(o=n[i],!Xy(t[o],e[o]))return!1;return!0}function Lf(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function Xy(t,e){if(Array.isArray(t)&&Array.isArray(e)){if(t.length!==e.length)return!1;let n=[...t].sort(),r=[...e].sort();return n.every((o,i)=>r[i]===o)}else return t===e}function eT(t){return t.length>0?t[t.length-1]:null}function br(t){return sa(t)?t:Ni(t)?ge(Promise.resolve(t)):q(t)}function eb(t){return sa(t)?Sc(t):Promise.resolve(t)}function qy(t,e,n){return tT[n.paths](t.root,e.root,n.matrixParams)&&tb[n.queryParams](t.queryParams,e.queryParams)&&!(n.fragment==="exact"&&t.fragment!==e.fragment)}function nT(t,e){return Bt(t,e)}function rb(t,e,n){if(!mr(t.segments,e.segments)||!Ec(t.segments,e.segments,n)||t.numberOfChildren!==e.numberOfChildren)return!1;for(let r in e.children)if(!t.children[r]||!rb(t.children[r],e.children[r],n))return!1;return!0}function rT(t,e){return Object.keys(e).length<=Object.keys(t).length&&Object.keys(e).every(n=>Xy(t[n],e[n]))}function ob(t,e,n){return ib(t,e,e.segments,n)}function ib(t,e,n,r){if(t.segments.length>n.length){let o=t.segments.slice(0,n.length);return!(!mr(o,n)||e.hasChildren()||!Ec(o,n,r))}else if(t.segments.length===n.length){if(!mr(t.segments,n)||!Ec(t.segments,n,r))return!1;for(let o in e.children)if(!t.children[o]||!ob(t.children[o],e.children[o],r))return!1;return!0}else{let o=n.slice(0,t.segments.length),i=n.slice(t.segments.length);return!mr(t.segments,o)||!Ec(t.segments,o,r)||!t.children[L]?!1:ib(t.children[L],e,i,r)}}function Ec(t,e,n){return e.every((r,o)=>tb[n](t[o].parameters,r.parameters))}function oT(t,e){return mr(t,e)&&t.every((n,r)=>Bt(n.parameters,e[r].parameters))}function mr(t,e){return t.length!==e.length?!1:t.every((n,r)=>n.path===e[r].path)}function iT(t,e){let n=[];return Object.entries(t.children).forEach(([r,o])=>{r===L&&(n=n.concat(e(o,r)))}),Object.entries(t.children).forEach(([r,o])=>{r!==L&&(n=n.concat(e(o,r)))}),n}function wc(t){return t.segments.map(e=>ab(e)).join("/")}function Hi(t,e){if(!t.hasChildren())return wc(t);if(e){let n=t.children[L]?Hi(t.children[L],!1):"",r=[];return Object.entries(t.children).forEach(([o,i])=>{o!==L&&r.push(`${o}:${Hi(i,!1)}`)}),r.length>0?`${n}(${r.join("//")})`:n}else{let n=iT(t,(r,o)=>o===L?[Hi(t.children[L],!1)]:[`${o}:${Hi(r,!1)}`]);return Object.keys(t.children).length===1&&t.children[L]!=null?`${wc(t)}/${n[0]}`:`${wc(t)}/(${n.join("//")})`}}function sb(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function bc(t){return sb(t).replace(/%3B/gi,";")}function aT(t){return encodeURI(t)}function Vf(t){return sb(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Ic(t){return decodeURIComponent(t)}function Wy(t){return Ic(t.replace(/\+/g,"%20"))}function ab(t){return`${Vf(t.path)}${cT(t.parameters)}`}function cT(t){return Object.entries(t).map(([e,n])=>`;${Vf(e)}=${Vf(n)}`).join("")}function lT(t){let e=Object.entries(t).map(([n,r])=>Array.isArray(r)?r.map(o=>`${bc(n)}=${bc(o)}`).join("&"):`${bc(n)}=${bc(r)}`).filter(n=>n);return e.length?`?${e.join("&")}`:""}function kf(t){let e=t.match(uT);return e?e[0]:""}function fT(t){let e=t.match(dT);return e?e[0]:""}function hT(t){let e=t.match(pT);return e?e[0]:""}function gT(t){let e=t.match(mT);return e?e[0]:""}function cb(t){return t.segments.length>0?new J([],{[L]:t}):t}function lb(t){let e={};for(let[r,o]of Object.entries(t.children)){let i=lb(o);if(r===L&&i.segments.length===0&&i.hasChildren())for(let[s,a]of Object.entries(i.children))e[s]=a;else(i.segments.length>0||i.hasChildren())&&(e[r]=i)}let n=new J(t.segments,e);return vT(n)}function vT(t){if(t.numberOfChildren===1&&t.children[L]){let e=t.children[L];return new J(t.segments.concat(e.segments),e.children)}return t}function go(t){return t instanceof Et}function ub(t,e,n=null,r=null,o=new kn){let i=db(t);return fb(i,e,n,r,o)}function db(t){let e;function n(i){let s={};for(let c of i.children){let l=n(c);s[c.outlet]=l}let a=new J(i.url,s);return i===t&&(e=a),a}let r=n(t.root),o=cb(r);return e??o}function fb(t,e,n,r,o){let i=t;for(;i.parent;)i=i.parent;if(e.length===0)return Af(i,i,i,n,r,o);let s=yT(e);if(s.toRoot())return Af(i,i,new J([],{}),n,r,o);let a=bT(s,i,t),c=a.processChildren?zi(a.segmentGroup,a.index,s.commands):hb(a.segmentGroup,a.index,s.commands);return Af(i,a.segmentGroup,c,n,r,o)}function Cc(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function Gi(t){return typeof t=="object"&&t!=null&&t.outlets}function Gy(t,e,n){t||="\u0275";let r=new Et;return r.queryParams={[t]:e},n.parse(n.serialize(r)).queryParams[t]}function Af(t,e,n,r,o,i){let s={};for(let[l,u]of Object.entries(r??{}))s[l]=Array.isArray(u)?u.map(d=>Gy(l,d,i)):Gy(l,u,i);let a;t===e?a=n:a=pb(t,e,n);let c=cb(lb(a));return new Et(c,s,o)}function pb(t,e,n){let r={};return Object.entries(t.children).forEach(([o,i])=>{i===e?r[o]=n:r[o]=pb(i,e,n)}),new J(t.segments,r)}function yT(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new xc(!0,0,t);let e=0,n=!1,r=t.reduce((o,i,s)=>{if(typeof i=="object"&&i!=null){if(i.outlets){let a={};return Object.entries(i.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...o,{outlets:a}]}if(i.segmentPath)return[...o,i.segmentPath]}return typeof i!="string"?[...o,i]:s===0?(i.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?n=!0:a===".."?e++:a!=""&&o.push(a))}),o):[...o,i]},[]);return new xc(n,e,r)}function bT(t,e,n){if(t.isAbsolute)return new ho(e,!0,0);if(!n)return new ho(e,!1,NaN);if(n.parent===null)return new ho(n,!0,0);let r=Cc(t.commands[0])?0:1,o=n.segments.length-1+r;return _T(n,o,t.numberOfDoubleDots)}function _T(t,e,n){let r=t,o=e,i=n;for(;i>o;){if(i-=o,r=r.parent,!r)throw new x(4005,!1);o=r.segments.length}return new ho(r,!1,o-i)}function ET(t){return Gi(t[0])?t[0].outlets:{[L]:t}}function hb(t,e,n){if(t??=new J([],{}),t.segments.length===0&&t.hasChildren())return zi(t,e,n);let r=wT(t,e,n),o=n.slice(r.commandIndex);if(r.match&&r.pathIndex<t.segments.length){let i=new J(t.segments.slice(0,r.pathIndex),{});return i.children[L]=new J(t.segments.slice(r.pathIndex),t.children),zi(i,0,o)}else return r.match&&o.length===0?new J(t.segments,{}):r.match&&!t.hasChildren()?jf(t,e,n):r.match?zi(t,0,o):jf(t,e,n)}function zi(t,e,n){if(n.length===0)return new J(t.segments,{});{let r=ET(n),o={};if(Object.keys(r).some(i=>i!==L)&&t.children[L]&&t.numberOfChildren===1&&t.children[L].segments.length===0){let i=zi(t.children[L],e,n);return new J(t.segments,i.children)}return Object.entries(r).forEach(([i,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(o[i]=hb(t.children[i],e,s))}),Object.entries(t.children).forEach(([i,s])=>{r[i]===void 0&&(o[i]=s)}),new J(t.segments,o)}}function wT(t,e,n){let r=0,o=e,i={match:!1,pathIndex:0,commandIndex:0};for(;o<t.segments.length;){if(r>=n.length)return i;let s=t.segments[o],a=n[r];if(Gi(a))break;let c=`${a}`,l=r<n.length-1?n[r+1]:null;if(o>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!Qy(c,l,s))return i;r+=2}else{if(!Qy(c,{},s))return i;r++}o++}return{match:!0,pathIndex:o,commandIndex:r}}function jf(t,e,n){let r=t.segments.slice(0,e),o=0;for(;o<n.length;){let i=n[o];if(Gi(i)){let c=IT(i.outlets);return new J(r,c)}if(o===0&&Cc(n[0])){let c=t.segments[e];r.push(new Rn(c.path,Ky(n[0]))),o++;continue}let s=Gi(i)?i.outlets[L]:`${i}`,a=o<n.length-1?n[o+1]:null;s&&a&&Cc(a)?(r.push(new Rn(s,Ky(a))),o+=2):(r.push(new Rn(s,{})),o++)}return new J(r,{})}function IT(t){let e={};return Object.entries(t).forEach(([n,r])=>{typeof r=="string"&&(r=[r]),r!==null&&(e[n]=jf(new J([],{}),0,r))}),e}function Ky(t){let e={};return Object.entries(t).forEach(([n,r])=>e[n]=`${r}`),e}function Qy(t,e,n){return t==n.path&&Bt(e,n.parameters)}function mb(t){return t instanceof _t&&(t.code===je.Redirect||t.code===je.SupersededByNewNavigation)}function ST(t){return!(t instanceof vo)&&!(t instanceof yo)&&!(t instanceof Zi)}function Uf(t,e){if(t===e.value)return e;for(let n of e.children){let r=Uf(t,n);if(r)return r}return null}function Hf(t,e){if(t===e.value)return[e];for(let n of e.children){let r=Hf(t,n);if(r.length)return r.unshift(e),r}return[]}function po(t){let e={};return t&&t.children.forEach(n=>e[n.value.outlet]=n),e}function gb(t,e){let n=CT(t,e),r=new Te([new Rn("",{})]),o=new Te({}),i=new Te({}),s=new Te({}),a=new Te(""),c=new An(r,o,s,a,i,L,t,n.root);return c.snapshot=n.root,new Yi(new lt(c,[]),n)}function CT(t,e){let n={},r={},o={},s=new bo([],n,o,"",r,L,t,null,{},e);return new Ji("",new lt(s,[]))}function Zf(t,e,n="emptyOnly"){let r,{routeConfig:o}=t;return e!==null&&(n==="always"||o?.path===""||!e.component&&!e.routeConfig?.loadComponent)?r={params:m(m({},e.params),t.params),data:m(m({},e.data),t.data),resolve:m(m(m(m({},t.data),e.data),o?.data),t._resolvedData)}:r={params:m({},t.params),data:m({},t.data),resolve:m(m({},t.data),t._resolvedData??{})},o&&yb(o)&&(r.resolve[ts]=o.title),r}function Yf(t,e){e.value._routerState=t,e.children.forEach(n=>Yf(t,n))}function vb(t){let e=t.children.length>0?` { ${t.children.map(vb).join(", ")} } `:"";return`${t.value}${e}`}function Of(t){if(t.snapshot){let e=t.snapshot,n=t._futureSnapshot;t.snapshot=n,Bt(e.queryParams,n.queryParams)||t.queryParamsSubject.next(n.queryParams),e.fragment!==n.fragment&&t.fragmentSubject.next(n.fragment),Bt(e.params,n.params)||t.paramsSubject.next(n.params),XD(e.url,n.url)||t.urlSubject.next(n.url),Bt(e.data,n.data)||t.dataSubject.next(n.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Bf(t,e){let n=Bt(t.params,e.params)&&oT(t.url,e.url),r=!t.parent!=!e.parent;return n&&!r&&(!t.parent||Bf(t.parent,e.parent))}function yb(t){return typeof t.title=="string"||t.title===null}function Xf(t){let e=t.children&&t.children.map(Xf),n=e?T(m({},t),{children:e}):m({},t);return!n.component&&!n.loadComponent&&(e||n.loadChildren)&&n.outlet&&n.outlet!==L&&(n.component=Jf),n}function xT(t,e,n){let r=Xi(t,e._root,n?n._root:void 0);return new Yi(r,e)}function Xi(t,e,n){if(n&&t.shouldReuseRoute(e.value,n.value.snapshot)){let r=n.value;r._futureSnapshot=e.value;let o=DT(t,e,n);return new lt(r,o)}else{if(t.shouldAttach(e.value)){let i=t.retrieve(e.value);if(i!==null){let s=i.route;return s.value._futureSnapshot=e.value,s.children=e.children.map(a=>Xi(t,a)),s}}let r=TT(e.value),o=e.children.map(i=>Xi(t,i));return new lt(r,o)}}function DT(t,e,n){return e.children.map(r=>{for(let o of n.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return Xi(t,r,o);return Xi(t,r)})}function TT(t){return new An(new Te(t.url),new Te(t.params),new Te(t.queryParams),new Te(t.fragment),new Te(t.data),t.outlet,t.component,t)}function Fc(t,e){let{redirectTo:n,navigationBehaviorOptions:r}=go(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,o=Eb(!1,je.Redirect);return o.url=n,o.navigationBehaviorOptions=r,o}function Eb(t,e){let n=new Error(`NavigationCancelingError: ${t||""}`);return n[_b]=!0,n.cancellationCode=e,n}function MT(t){return wb(t)&&go(t.url)}function wb(t){return!!t&&t[_b]}function NT(t,e,n){let r=t._root,o=e?e._root:null;return Bi(r,o,n,[r.value])}function RT(t){let e=t.routeConfig?t.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:t,guards:e}}function Io(t,e){let n=Symbol(),r=e.get(t,n);return r===n?typeof t=="function"&&!ku(t)?t:e.get(t):r}function Bi(t,e,n,r,o={canDeactivateChecks:[],canActivateChecks:[]}){let i=po(e);return t.children.forEach(s=>{kT(s,i[s.value.outlet],n,r.concat([s.value]),o),delete i[s.value.outlet]}),Object.entries(i).forEach(([s,a])=>Wi(a,n.getContext(s),o)),o}function kT(t,e,n,r,o={canDeactivateChecks:[],canActivateChecks:[]}){let i=t.value,s=e?e.value:null,a=n?n.getContext(t.value.outlet):null;if(s&&i.routeConfig===s.routeConfig){let c=AT(s,i,i.routeConfig.runGuardsAndResolvers);c?o.canActivateChecks.push(new jc(r)):(i.data=s.data,i._resolvedData=s._resolvedData),i.component?Bi(t,e,a?a.children:null,r,o):Bi(t,e,n,r,o),c&&a&&a.outlet&&a.outlet.isActivated&&o.canDeactivateChecks.push(new mo(a.outlet.component,s))}else s&&Wi(e,a,o),o.canActivateChecks.push(new jc(r)),i.component?Bi(t,null,a?a.children:null,r,o):Bi(t,null,n,r,o);return o}function AT(t,e,n){if(typeof n=="function")return $e(e._environmentInjector,()=>n(t,e));switch(n){case"pathParamsChange":return!mr(t.url,e.url);case"pathParamsOrQueryParamsChange":return!mr(t.url,e.url)||!Bt(t.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Bf(t,e)||!Bt(t.queryParams,e.queryParams);default:return!Bf(t,e)}}function Wi(t,e,n){let r=po(t),o=t.value;Object.entries(r).forEach(([i,s])=>{o.component?e?Wi(s,e.children.getContext(i),n):Wi(s,null,n):Wi(s,e,n)}),o.component?e&&e.outlet&&e.outlet.isActivated?n.canDeactivateChecks.push(new mo(e.outlet.component,o)):n.canDeactivateChecks.push(new mo(null,o)):n.canDeactivateChecks.push(new mo(null,o))}function os(t){return typeof t=="function"}function OT(t){return typeof t=="boolean"}function PT(t){return t&&os(t.canLoad)}function LT(t){return t&&os(t.canActivate)}function $T(t){return t&&os(t.canActivateChild)}function VT(t){return t&&os(t.canDeactivate)}function FT(t){return t&&os(t.canMatch)}function Ib(t){return t instanceof Gn||t?.name==="EmptyError"}function Eo(){return Le(t=>au(t.map(e=>e.pipe(Ge(1),fu(_c)))).pipe(ne(e=>{for(let n of e)if(n!==!0){if(n===_c)return _c;if(n===!1||jT(n))return n}return!0}),pt(e=>e!==_c),Ge(1)))}function jT(t){return go(t)||t instanceof _o}function Sb(t){return t.aborted?q(void 0).pipe(Ge(1)):new z(e=>{let n=()=>{e.next(),e.complete()};return t.addEventListener("abort",n),()=>t.removeEventListener("abort",n)})}function Cb(t){return Yo(Sb(t))}function UT(t){return We(e=>{let{targetSnapshot:n,currentSnapshot:r,guards:{canActivateChecks:o,canDeactivateChecks:i}}=e;return i.length===0&&o.length===0?q(T(m({},e),{guardsResult:!0})):HT(i,n,r).pipe(We(s=>s&&OT(s)?BT(n,o,t):q(s)),ne(s=>T(m({},e),{guardsResult:s})))})}function HT(t,e,n){return ge(t).pipe(We(r=>KT(r.component,r.route,n,e)),Qt(r=>r!==!0,!0))}function BT(t,e,n){return ge(e).pipe(ua(r=>qr(qT(r.route.parent,n),zT(r.route,n),GT(t,r.path),WT(t,r.route))),Qt(r=>r!==!0,!0))}function zT(t,e){return t!==null&&e&&e(new Pc(t)),q(!0)}function qT(t,e){return t!==null&&e&&e(new Ac(t)),q(!0)}function WT(t,e){let n=e.routeConfig?e.routeConfig.canActivate:null;if(!n||n.length===0)return q(!0);let r=n.map(o=>Qo(()=>{let i=e._environmentInjector,s=Io(o,i),a=LT(s)?s.canActivate(e,t):$e(i,()=>s(e,t));return br(a).pipe(Qt())}));return q(r).pipe(Eo())}function GT(t,e){let n=e[e.length-1],o=e.slice(0,e.length-1).reverse().map(i=>RT(i)).filter(i=>i!==null).map(i=>Qo(()=>{let s=i.guards.map(a=>{let c=i.node._environmentInjector,l=Io(a,c),u=$T(l)?l.canActivateChild(n,t):$e(c,()=>l(n,t));return br(u).pipe(Qt())});return q(s).pipe(Eo())}));return q(o).pipe(Eo())}function KT(t,e,n,r){let o=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!o||o.length===0)return q(!0);let i=o.map(s=>{let a=e._environmentInjector,c=Io(s,a),l=VT(c)?c.canDeactivate(t,e,n,r):$e(a,()=>c(t,e,n,r));return br(l).pipe(Qt())});return q(i).pipe(Eo())}function QT(t,e,n,r,o){let i=e.canLoad;if(i===void 0||i.length===0)return q(!0);let s=i.map(a=>{let c=Io(a,t),l=PT(c)?c.canLoad(e,n):$e(t,()=>c(e,n)),u=br(l);return o?u.pipe(Cb(o)):u});return q(s).pipe(Eo(),xb(r))}function xb(t){return zl(ht(e=>{if(typeof e!="boolean")throw Fc(t,e)}),ne(e=>e===!0))}function ZT(t,e,n,r,o,i){let s=e.canMatch;if(!s||s.length===0)return q(!0);let a=s.map(c=>{let l=Io(c,t),u=FT(l)?l.canMatch(e,n,o):$e(t,()=>l(e,n,o));return br(u).pipe(Cb(i))});return q(a).pipe(Eo(),xb(r))}function YT(t){throw new x(4e3,!1)}function JT(t){throw Eb(!1,je.GuardRejected)}function XT(t,e,n){if(typeof t=="string")return Promise.resolve(t);let r=t;return Sc(br($e(n,()=>r(e))))}function eM(t,e){return t.providers&&!t._injector&&(t._injector=Mi(t.providers,e,`Route: ${t.path}`)),t._injector??e}function Rt(t){return t.outlet||L}function tM(t,e){let n=t.filter(r=>Rt(r)===e);return n.push(...t.filter(r=>Rt(r)!==e)),n}function Db(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function nM(t,e,n,r,o,i,s){let a=Tb(t,e,n);if(!a.matched)return q(a);let c=Db(i(a));return r=eM(e,r),ZT(r,e,n,o,c,s).pipe(ne(l=>l===!0?a:m({},Gf)))}function Tb(t,e,n){if(e.path==="")return e.pathMatch==="full"&&(t.hasChildren()||n.length>0)?m({},Gf):{matched:!0,consumedSegments:[],remainingSegments:n,parameters:{},positionalParamSegments:{}};let o=(e.matcher||Jy)(n,t,e);if(!o)return m({},Gf);let i={};Object.entries(o.posParams??{}).forEach(([a,c])=>{i[a]=c.path});let s=o.consumed.length>0?m(m({},i),o.consumed[o.consumed.length-1].parameters):i;return{matched:!0,consumedSegments:o.consumed,remainingSegments:n.slice(o.consumed.length),parameters:s,positionalParamSegments:o.posParams??{}}}function Zy(t,e,n,r,o){return n.length>0&&iM(t,n,r,o)?{segmentGroup:new J(e,oM(r,new J(n,t.children))),slicedSegments:[]}:n.length===0&&sM(t,n,r)?{segmentGroup:new J(t.segments,rM(t,n,r,t.children)),slicedSegments:n}:{segmentGroup:new J(t.segments,t.children),slicedSegments:n}}function rM(t,e,n,r){let o={};for(let i of n)if(Bc(t,e,i)&&!r[Rt(i)]){let s=new J([],{});o[Rt(i)]=s}return m(m({},r),o)}function oM(t,e){let n={};n[L]=e;for(let r of t)if(r.path===""&&Rt(r)!==L){let o=new J([],{});n[Rt(r)]=o}return n}function iM(t,e,n,r){return n.some(o=>!Bc(t,e,o)||!(Rt(o)!==L)?!1:!(r!==void 0&&Rt(o)===r))}function sM(t,e,n){return n.some(r=>Bc(t,e,r))}function Bc(t,e,n){return(t.hasChildren()||e.length>0)&&n.pathMatch==="full"?!1:n.path===""}function aM(t,e,n){return e.length===0&&!t.children[n]}async function cM(t,e,n,r,o,i,s="emptyOnly",a){return new Qf(t,e,n,r,o,s,i,a).recognize()}function uM(t){t.sort((e,n)=>e.value.outlet===L?-1:n.value.outlet===L?1:e.value.outlet.localeCompare(n.value.outlet))}function dM(t){let e=t.value.routeConfig;return e&&e.path===""}function Mb(t){let e=[],n=new Set;for(let r of t){if(!dM(r)){e.push(r);continue}let o=e.find(i=>r.value.routeConfig===i.value.routeConfig);o!==void 0?(o.children.push(...r.children),n.add(o)):e.push(r)}for(let r of n){let o=Mb(r.children);e.push(new lt(r.value,o))}return e.filter(r=>!n.has(r))}function fM(t){return t.data||{}}function pM(t){return t.resolve||{}}function hM(t,e,n,r,o,i,s){return We(async a=>{let{state:c,tree:l}=await cM(t,e,n,r,a.extractedUrl,o,i,s);return T(m({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function mM(t){return We(e=>{let{targetSnapshot:n,guards:{canActivateChecks:r}}=e;if(!r.length)return q(e);let o=new Set(r.map(a=>a.route)),i=new Set;for(let a of o)if(!i.has(a))for(let c of Nb(a))i.add(c);let s=0;return ge(i).pipe(ua(a=>o.has(a)?gM(a,n,t):(a.data=Zf(a,a.parent,t).resolve,q(void 0))),ht(()=>s++),da(1),We(a=>s===i.size?q(e):Me))})}function Nb(t){let e=t.children.map(n=>Nb(n)).flat();return[t,...e]}function gM(t,e,n){let r=t.routeConfig,o=t._resolve;return r?.title!==void 0&&!yb(r)&&(o[ts]=r.title),Qo(()=>(t.data=Zf(t,t.parent,n).resolve,vM(o,t,e).pipe(ne(i=>(t._resolvedData=i,t.data=m(m({},t.data),i),null)))))}function vM(t,e,n){let r=Lf(t);if(r.length===0)return q({});let o={};return ge(r).pipe(We(i=>yM(t[i],e,n).pipe(Qt(),ht(s=>{if(s instanceof _o)throw Fc(new kn,s);o[i]=s}))),da(1),ne(()=>o),Zo(i=>Ib(i)?Me:su(i)))}function yM(t,e,n){let r=e._environmentInjector,o=Io(t,r),i=o.resolve?o.resolve(e,n):$e(r,()=>o(e,n));return br(i)}function Yy(t){return Le(e=>{let n=t(e);return n?ge(n).pipe(ne(()=>e)):q(e)})}async function Ab(t,e,n,r){let o=await eb($e(n,()=>t.loadChildren())),i=await Pb(Ob(o)),s;i instanceof oc||Array.isArray(i)?s=i:s=await e.compileModuleAsync(i),r&&r(t);let a,c,l=!1,u;return Array.isArray(s)?(c=s,l=!0):(a=s.create(n).injector,u=s,c=a.get(ss,[],{optional:!0,self:!0}).flat()),{routes:c.map(Xf),injector:a,factory:u}}function bM(t){return t&&typeof t=="object"&&"default"in t}function Ob(t){return bM(t)?t.default:t}async function Pb(t){return t}function wM(t){return t!==qi}function np(t,e){t.events.pipe(pt(n=>n instanceof cn||n instanceof _t||n instanceof yr||n instanceof ln),ne(n=>n instanceof cn||n instanceof ln?0:(n instanceof _t?n.code===je.Redirect||n.code===je.SupersededByNewNavigation:!1)?2:1),pt(n=>n!==2),Ge(1)).subscribe(()=>{e()})}function CM(t){for(let e=0;e<t.length;e++)if(t[e]==null)throw new x(4008,!1)}var L,ts,Pf,tT,tb,nb,$f,Et,J,Rn,ns,kn,sT,uT,dT,pT,mT,Ff,xc,ho,qi,Se,ut,vr,cn,je,Ki,_t,ln,yr,Qi,Dc,Tc,Mc,Nc,Rc,kc,Ac,Oc,Pc,Lc,vo,Zi,yo,$c,wo,Vc,lt,Yi,An,bo,Ji,bb,rs,zf,Hc,Jf,_o,_b,qf,jc,mo,_c,an,es,Wf,Gf,Kf,lM,Qf,ep,Rb,is,ss,kb,zc,_M,Lb,EM,$b,Vb,Fb,jb,Uc,IM,tp,SM,qc,rp=f(()=>{"use strict";$i();fe();fe();Kn();Aa();Nf();L="primary",ts=Symbol("RouteTitle"),Pf=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let n=this.params[e];return Array.isArray(n)?n[0]:n}return null}getAll(e){if(this.has(e)){let n=this.params[e];return Array.isArray(n)?n:[n]}return[]}get keys(){return Object.keys(this.params)}};tT={exact:rb,subset:ob},tb={exact:nT,subset:rT,ignored:()=>!0},nb={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},$f={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};Et=class{root;queryParams;fragment;_queryParamMap;constructor(e=new J([],{}),n={},r=null){this.root=e,this.queryParams=n,this.fragment=r}get queryParamMap(){return this._queryParamMap??=gr(this.queryParams),this._queryParamMap}toString(){return sT.serialize(this)}},J=class{segments;children;parent=null;constructor(e,n){this.segments=e,this.children=n,Object.values(n).forEach(r=>r.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return wc(this)}},Rn=class{path;parameters;_parameterMap;constructor(e,n){this.path=e,this.parameters=n}get parameterMap(){return this._parameterMap??=gr(this.parameters),this._parameterMap}toString(){return ab(this)}};ns=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>new kn,providedIn:"root"})}return t})(),kn=class{parse(e){let n=new Ff(e);return new Et(n.parseRootSegment(),n.parseQueryParams(),n.parseFragment())}serialize(e){let n=`/${Hi(e.root,!0)}`,r=lT(e.queryParams),o=typeof e.fragment=="string"?`#${aT(e.fragment)}`:"";return`${n}${r}${o}`}},sT=new kn;uT=/^[^\/()?;#]+/;dT=/^[^\/()?;=#]+/;pT=/^[^=?&#]+/;mT=/^[^&#]+/;Ff=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new J([],{}):new J([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(e=0){if(e>50)throw new x(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let n=[];for(this.peekStartsWith("(")||n.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),n.push(this.parseSegment());let r={};this.peekStartsWith("/(")&&(this.capture("/"),r=this.parseParens(!0,e));let o={};return this.peekStartsWith("(")&&(o=this.parseParens(!1,e)),(n.length>0||Object.keys(r).length>0)&&(o[L]=new J(n,r)),o}parseSegment(){let e=kf(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new x(4009,!1);return this.capture(e),new Rn(Ic(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let n=fT(this.remaining);if(!n)return;this.capture(n);let r="";if(this.consumeOptional("=")){let o=kf(this.remaining);o&&(r=o,this.capture(r))}e[Ic(n)]=Ic(r)}parseQueryParam(e){let n=hT(this.remaining);if(!n)return;this.capture(n);let r="";if(this.consumeOptional("=")){let s=gT(this.remaining);s&&(r=s,this.capture(r))}let o=Wy(n),i=Wy(r);if(e.hasOwnProperty(o)){let s=e[o];Array.isArray(s)||(s=[s],e[o]=s),s.push(i)}else e[o]=i}parseParens(e,n){let r={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let o=kf(this.remaining),i=this.remaining[o.length];if(i!=="/"&&i!==")"&&i!==";")throw new x(4010,!1);let s;o.indexOf(":")>-1?(s=o.slice(0,o.indexOf(":")),this.capture(s),this.capture(":")):e&&(s=L);let a=this.parseChildren(n+1);r[s??L]=Object.keys(a).length===1&&a[L]?a[L]:new J([],a),this.consumeOptional("//")}return r}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new x(4011,!1)}};xc=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,n,r){if(this.isAbsolute=e,this.numberOfDoubleDots=n,this.commands=r,e&&r.length>0&&Cc(r[0]))throw new x(4003,!1);let o=r.find(Gi);if(o&&o!==eT(r))throw new x(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};ho=class{segmentGroup;processChildren;index;constructor(e,n,r){this.segmentGroup=e,this.processChildren=n,this.index=r}};qi="imperative",Se=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Se||{}),ut=class{id;url;constructor(e,n){this.id=e,this.url=n}},vr=class extends ut{type=Se.NavigationStart;navigationTrigger;restoredState;constructor(e,n,r="imperative",o=null){super(e,n),this.navigationTrigger=r,this.restoredState=o}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},cn=class extends ut{urlAfterRedirects;type=Se.NavigationEnd;constructor(e,n,r){super(e,n),this.urlAfterRedirects=r}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},je=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(je||{}),Ki=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(Ki||{}),_t=class extends ut{reason;code;type=Se.NavigationCancel;constructor(e,n,r,o){super(e,n),this.reason=r,this.code=o}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};ln=class extends ut{reason;code;type=Se.NavigationSkipped;constructor(e,n,r,o){super(e,n),this.reason=r,this.code=o}},yr=class extends ut{error;target;type=Se.NavigationError;constructor(e,n,r,o){super(e,n),this.error=r,this.target=o}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Qi=class extends ut{urlAfterRedirects;state;type=Se.RoutesRecognized;constructor(e,n,r,o){super(e,n),this.urlAfterRedirects=r,this.state=o}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Dc=class extends ut{urlAfterRedirects;state;type=Se.GuardsCheckStart;constructor(e,n,r,o){super(e,n),this.urlAfterRedirects=r,this.state=o}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Tc=class extends ut{urlAfterRedirects;state;shouldActivate;type=Se.GuardsCheckEnd;constructor(e,n,r,o,i){super(e,n),this.urlAfterRedirects=r,this.state=o,this.shouldActivate=i}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Mc=class extends ut{urlAfterRedirects;state;type=Se.ResolveStart;constructor(e,n,r,o){super(e,n),this.urlAfterRedirects=r,this.state=o}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Nc=class extends ut{urlAfterRedirects;state;type=Se.ResolveEnd;constructor(e,n,r,o){super(e,n),this.urlAfterRedirects=r,this.state=o}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Rc=class{route;type=Se.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},kc=class{route;type=Se.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Ac=class{snapshot;type=Se.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Oc=class{snapshot;type=Se.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Pc=class{snapshot;type=Se.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Lc=class{snapshot;type=Se.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},vo=class{},Zi=class{},yo=class{url;navigationBehaviorOptions;constructor(e,n){this.url=e,this.navigationBehaviorOptions=n}};$c=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new wo(this.rootInjector)}},wo=(()=>{class t{rootInjector;contexts=new Map;constructor(n){this.rootInjector=n}onChildOutletCreated(n,r){let o=this.getOrCreateContext(n);o.outlet=r,this.contexts.set(n,o)}onChildOutletDestroyed(n){let r=this.getContext(n);r&&(r.outlet=null,r.attachRef=null)}onOutletDeactivated(){let n=this.contexts;return this.contexts=new Map,n}onOutletReAttached(n){this.contexts=n}getOrCreateContext(n){let r=this.getContext(n);return r||(r=new $c(this.rootInjector),this.contexts.set(n,r)),r}getContext(n){return this.contexts.get(n)||null}static \u0275fac=function(r){return new(r||t)(F(ye))};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Vc=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let n=this.pathFromRoot(e);return n.length>1?n[n.length-2]:null}children(e){let n=Uf(e,this._root);return n?n.children.map(r=>r.value):[]}firstChild(e){let n=Uf(e,this._root);return n&&n.children.length>0?n.children[0].value:null}siblings(e){let n=Hf(e,this._root);return n.length<2?[]:n[n.length-2].children.map(o=>o.value).filter(o=>o!==e)}pathFromRoot(e){return Hf(e,this._root).map(n=>n.value)}};lt=class{value;children;constructor(e,n){this.value=e,this.children=n}toString(){return`TreeNode(${this.value})`}};Yi=class extends Vc{snapshot;constructor(e,n){super(e),this.snapshot=n,Yf(this,e)}toString(){return this.snapshot.toString()}};An=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,n,r,o,i,s,a,c){this.urlSubject=e,this.paramsSubject=n,this.queryParamsSubject=r,this.fragmentSubject=o,this.dataSubject=i,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(ne(l=>l[ts]))??q(void 0),this.url=e,this.params=n,this.queryParams=r,this.fragment=o,this.data=i}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ne(e=>gr(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ne(e=>gr(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};bo=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[ts]}constructor(e,n,r,o,i,s,a,c,l,u){this.url=e,this.params=n,this.queryParams=r,this.fragment=o,this.data=i,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=gr(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=gr(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(r=>r.toString()).join("/"),n=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${n}')`}},Ji=class extends Vc{url;constructor(e,n){super(n),this.url=e,Yf(this,n)}toString(){return vb(this._root)}};bb=new k(""),rs=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=L;activateEvents=new Ke;deactivateEvents=new Ke;attachEvents=new Ke;detachEvents=new Ke;routerOutletData=Iy();parentContexts=y(wo);location=y(rc);changeDetector=y(Cy);inputBinder=y(Hc,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(n){if(n.name){let{firstChange:r,previousValue:o}=n.name;if(r)return;this.isTrackedInParentContexts(o)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(o)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(n){return this.parentContexts.getContext(n)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let n=this.parentContexts.getContext(this.name);n?.route&&(n.attachRef?this.attach(n.attachRef,n.route):this.activateWith(n.route,n.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new x(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new x(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new x(4012,!1);this.location.detach();let n=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(n.instance),n}attach(n,r){this.activated=n,this._activatedRoute=r,this.location.insert(n.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(n.instance)}deactivate(){if(this.activated){let n=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(n)}}activateWith(n,r){if(this.isActivated)throw new x(4013,!1);this._activatedRoute=n;let o=this.location,s=n.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new zf(n,a,o.injector,this.routerOutletData);this.activated=o.createComponent(s,{index:o.length,injector:c,environmentInjector:r}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(r){return new(r||t)};static \u0275dir=cf({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Wd]})}return t})(),zf=class{route;childContexts;parent;outletData;constructor(e,n,r,o){this.route=e,this.childContexts=n,this.parent=r,this.outletData=o}get(e,n){return e===An?this.route:e===wo?this.childContexts:e===bb?this.outletData:this.parent.get(e,n)}},Hc=new k(""),Jf=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=pr({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(r,o){r&1&&hr(0,"router-outlet")},dependencies:[rs],encapsulation:2})}return t})();_o=class{redirectTo;navigationBehaviorOptions;constructor(e,n){this.redirectTo=e,this.navigationBehaviorOptions=n}},_b="ngNavigationCancelingError";qf=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,n,r,o,i){this.routeReuseStrategy=e,this.futureState=n,this.currState=r,this.forwardEvent=o,this.inputBindingEnabled=i}activate(e){let n=this.futureState._root,r=this.currState?this.currState._root:null;this.deactivateChildRoutes(n,r,e),Of(this.futureState.root),this.activateChildRoutes(n,r,e)}deactivateChildRoutes(e,n,r){let o=po(n);e.children.forEach(i=>{let s=i.value.outlet;this.deactivateRoutes(i,o[s],r),delete o[s]}),Object.values(o).forEach(i=>{this.deactivateRouteAndItsChildren(i,r)})}deactivateRoutes(e,n,r){let o=e.value,i=n?n.value:null;if(o===i)if(o.component){let s=r.getContext(o.outlet);s&&this.deactivateChildRoutes(e,n,s.children)}else this.deactivateChildRoutes(e,n,r);else i&&this.deactivateRouteAndItsChildren(n,r)}deactivateRouteAndItsChildren(e,n){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,n):this.deactivateRouteAndOutlet(e,n)}detachAndStoreRouteSubtree(e,n){let r=n.getContext(e.value.outlet),o=r&&e.value.component?r.children:n,i=po(e);for(let s of Object.values(i))this.deactivateRouteAndItsChildren(s,o);if(r&&r.outlet){let s=r.outlet.detach(),a=r.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:s,route:e,contexts:a})}}deactivateRouteAndOutlet(e,n){let r=n.getContext(e.value.outlet),o=r&&e.value.component?r.children:n,i=po(e);for(let s of Object.values(i))this.deactivateRouteAndItsChildren(s,o);r&&(r.outlet&&(r.outlet.deactivate(),r.children.onOutletDeactivated()),r.attachRef=null,r.route=null)}activateChildRoutes(e,n,r){let o=po(n);e.children.forEach(i=>{this.activateRoutes(i,o[i.value.outlet],r),this.forwardEvent(new Lc(i.value.snapshot))}),e.children.length&&this.forwardEvent(new Oc(e.value.snapshot))}activateRoutes(e,n,r){let o=e.value,i=n?n.value:null;if(Of(o),o===i)if(o.component){let s=r.getOrCreateContext(o.outlet);this.activateChildRoutes(e,n,s.children)}else this.activateChildRoutes(e,n,r);else if(o.component){let s=r.getOrCreateContext(o.outlet);if(this.routeReuseStrategy.shouldAttach(o.snapshot)){let a=this.routeReuseStrategy.retrieve(o.snapshot);this.routeReuseStrategy.store(o.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Of(a.route.value),this.activateChildRoutes(e,null,s.children)}else s.attachRef=null,s.route=o,s.outlet&&s.outlet.activateWith(o,s.injector),this.activateChildRoutes(e,null,s.children)}else this.activateChildRoutes(e,null,r)}},jc=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},mo=class{component;route;constructor(e,n){this.component=e,this.route=n}};_c=Symbol("INITIAL_VALUE");an=class t extends Error{segmentGroup;constructor(e){super(),this.segmentGroup=e||null,Object.setPrototypeOf(this,t.prototype)}},es=class t extends Error{urlTree;constructor(e){super(),this.urlTree=e,Object.setPrototypeOf(this,t.prototype)}};Wf=class{urlSerializer;urlTree;constructor(e,n){this.urlSerializer=e,this.urlTree=n}async lineralizeSegments(e,n){let r=[],o=n.root;for(;;){if(r=r.concat(o.segments),o.numberOfChildren===0)return r;if(o.numberOfChildren>1||!o.children[L])throw YT(`${e.redirectTo}`);o=o.children[L]}}async applyRedirectCommands(e,n,r,o,i){let s=await XT(n,o,i);if(s instanceof Et)throw new es(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),e,r);if(s[0]==="/")throw new es(a);return a}applyRedirectCreateUrlTree(e,n,r,o){let i=this.createSegmentGroup(e,n.root,r,o);return new Et(i,this.createQueryParams(n.queryParams,this.urlTree.queryParams),n.fragment)}createQueryParams(e,n){let r={};return Object.entries(e).forEach(([o,i])=>{if(typeof i=="string"&&i[0]===":"){let a=i.substring(1);r[o]=n[a]}else r[o]=i}),r}createSegmentGroup(e,n,r,o){let i=this.createSegments(e,n.segments,r,o),s={};return Object.entries(n.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(e,c,r,o)}),new J(i,s)}createSegments(e,n,r,o){return n.map(i=>i.path[0]===":"?this.findPosParam(e,i,o):this.findOrReturn(i,r))}findPosParam(e,n,r){let o=r[n.path.substring(1)];if(!o)throw new x(4001,!1);return o}findOrReturn(e,n){let r=0;for(let o of n){if(o.path===e.path)return n.splice(r),o;r++}return e}};Gf={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};Kf=class{};lM=31,Qf=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(e,n,r,o,i,s,a,c){this.injector=e,this.configLoader=n,this.rootComponentType=r,this.config=o,this.urlTree=i,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Wf(this.urlSerializer,this.urlTree)}noMatchError(e){return new x(4002,`'${e.segmentGroup}'`)}async recognize(){let e=Zy(this.urlTree.root,[],[],this.config).segmentGroup,{children:n,rootSnapshot:r}=await this.match(e),o=new lt(r,n),i=new Ji("",o),s=ub(r,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,i.url=this.urlSerializer.serialize(s),{state:i,tree:s}}async match(e){let n=new bo([],Object.freeze({}),Object.freeze(m({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),L,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,e,L,n),rootSnapshot:n}}catch(r){if(r instanceof es)return this.urlTree=r.urlTree,this.match(r.urlTree.root);throw r instanceof an?this.noMatchError(r):r}}async processSegmentGroup(e,n,r,o,i){if(r.segments.length===0&&r.hasChildren())return this.processChildren(e,n,r,i);let s=await this.processSegment(e,n,r,r.segments,o,!0,i);return s instanceof lt?[s]:[]}async processChildren(e,n,r,o){let i=[];for(let c of Object.keys(r.children))c==="primary"?i.unshift(c):i.push(c);let s=[];for(let c of i){let l=r.children[c],u=tM(n,c),d=await this.processSegmentGroup(e,u,l,c,o);s.push(...d)}let a=Mb(s);return uM(a),a}async processSegment(e,n,r,o,i,s,a){for(let c of n)try{return await this.processSegmentAgainstRoute(c._injector??e,n,c,r,o,i,s,a)}catch(l){if(l instanceof an||Ib(l))continue;throw l}if(aM(r,o,i))return new Kf;throw new an(r)}async processSegmentAgainstRoute(e,n,r,o,i,s,a,c){if(Rt(r)!==s&&(s===L||!Bc(o,i,r)))throw new an(o);if(r.redirectTo===void 0)return this.matchSegmentAgainstRoute(e,o,r,i,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(e,o,n,r,i,s,c);throw new an(o)}async expandSegmentAgainstRouteUsingRedirect(e,n,r,o,i,s,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:h}=Tb(n,o,i);if(!c)throw new an(n);typeof o.redirectTo=="string"&&o.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>lM&&(this.allowRedirects=!1));let p=this.createSnapshot(e,o,i,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let g=await this.applyRedirects.applyRedirectCommands(u,o.redirectTo,d,Db(p),e),E=await this.applyRedirects.lineralizeSegments(o,g);return this.processSegment(e,r,n,E.concat(h),s,!1,a)}createSnapshot(e,n,r,o,i){let s=new bo(r,o,Object.freeze(m({},this.urlTree.queryParams)),this.urlTree.fragment,fM(n),Rt(n),n.component??n._loadedComponent??null,n,pM(n),e),a=Zf(s,i,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(e,n,r,o,i,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=I=>this.createSnapshot(e,r,I.consumedSegments,I.parameters,s),c=await Sc(nM(n,r,o,e,this.urlSerializer,a,this.abortSignal));if(r.path==="**"&&(n.children={}),!c?.matched)throw new an(n);e=r._injector??e;let{routes:l}=await this.getChildConfig(e,r,o),u=r._loadedInjector??e,{parameters:d,consumedSegments:h,remainingSegments:p}=c,g=this.createSnapshot(e,r,h,d,s),{segmentGroup:E,slicedSegments:b}=Zy(n,h,p,l,i);if(b.length===0&&E.hasChildren()){let I=await this.processChildren(u,l,E,g);return new lt(g,I)}if(l.length===0&&b.length===0)return new lt(g,[]);let D=Rt(r)===i,V=await this.processSegment(u,l,E,b,D?L:i,!0,g);return new lt(g,V instanceof lt?[V]:[])}async getChildConfig(e,n,r){if(n.children)return{routes:n.children,injector:e};if(n.loadChildren){if(n._loadedRoutes!==void 0){let i=n._loadedNgModuleFactory;return i&&!n._loadedInjector&&(n._loadedInjector=i.create(e).injector),{routes:n._loadedRoutes,injector:n._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Sc(QT(e,n,r,this.urlSerializer,this.abortSignal))){let i=await this.configLoader.loadChildren(e,n);return n._loadedRoutes=i.routes,n._loadedInjector=i.injector,n._loadedNgModuleFactory=i.factory,i}throw JT(n)}return{routes:[],injector:e}}};ep=(()=>{class t{buildTitle(n){let r,o=n.root;for(;o!==void 0;)r=this.getResolvedTitleForRoute(o)??r,o=o.children.find(i=>i.outlet===L);return r}getResolvedTitleForRoute(n){return n.data[ts]}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>y(Rb),providedIn:"root"})}return t})(),Rb=(()=>{class t extends ep{title;constructor(n){super(),this.title=n}updateTitle(n){let r=this.buildTitle(n);r!==void 0&&this.title.setTitle(r)}static \u0275fac=function(r){return new(r||t)(F(zy))};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),is=new k("",{factory:()=>({})}),ss=new k(""),kb=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=y(pf);async loadComponent(n,r){if(this.componentLoaders.get(r))return this.componentLoaders.get(r);if(r._loadedComponent)return Promise.resolve(r._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(r);let o=(async()=>{try{let i=await eb($e(n,()=>r.loadComponent())),s=await Pb(Ob(i));return this.onLoadEndListener&&this.onLoadEndListener(r),r._loadedComponent=s,s}finally{this.componentLoaders.delete(r)}})();return this.componentLoaders.set(r,o),o}loadChildren(n,r){if(this.childrenLoaders.get(r))return this.childrenLoaders.get(r);if(r._loadedRoutes)return Promise.resolve({routes:r._loadedRoutes,injector:r._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(r);let o=(async()=>{try{let i=await Ab(r,this.compiler,n,this.onLoadEndListener);return r._loadedRoutes=i.routes,r._loadedInjector=i.injector,r._loadedNgModuleFactory=i.factory,i}finally{this.childrenLoaders.delete(r)}})();return this.childrenLoaders.set(r,o),o}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();zc=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>y(_M),providedIn:"root"})}return t})(),_M=(()=>{class t{shouldProcessUrl(n){return!0}extract(n){return n}merge(n,r){return n}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Lb=new k(""),EM=()=>{},$b=new k(""),Vb=(()=>{class t{currentNavigation=Fe(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=Fe(null);events=new me;transitionAbortWithErrorSubject=new me;configLoader=y(kb);environmentInjector=y(ye);destroyRef=y(Cn);urlSerializer=y(ns);rootContexts=y(wo);location=y(fo);inputBindingEnabled=y(Hc,{optional:!0})!==null;titleStrategy=y(ep);options=y(is,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=y(zc);createViewTransition=y(Lb,{optional:!0});navigationErrorHandler=y($b,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>q(void 0);rootComponentType=null;destroyed=!1;constructor(){let n=o=>this.events.next(new Rc(o)),r=o=>this.events.next(new kc(o));this.configLoader.onLoadEndListener=r,this.configLoader.onLoadStartListener=n,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(n){let r=++this.navigationId;Mn(()=>{this.transitions?.next(T(m({},n),{extractedUrl:this.urlHandlingStrategy.extract(n.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:r,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(n){return this.transitions=new Te(null),this.transitions.pipe(pt(r=>r!==null),Le(r=>{let o=!1,i=new AbortController,s=()=>!o&&this.currentTransition?.id===r.id;return q(r).pipe(Le(a=>{if(this.navigationId>r.id)return this.cancelNavigationTransition(r,"",je.SupersededByNewNavigation),Me;this.currentTransition=r;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?T(m({},c),{previousNavigation:null}):null,abort:()=>i.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!n.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??n.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new ln(a.id,this.urlSerializer.serialize(a.rawUrl),"",Ki.IgnoredSameUrlNavigation)),a.resolve(!1),Me;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return q(a).pipe(Le(d=>(this.events.next(new vr(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),d.id!==this.navigationId?Me:Promise.resolve(d))),hM(this.environmentInjector,this.configLoader,this.rootComponentType,n.config,this.urlSerializer,this.paramsInheritanceStrategy,i.signal),ht(d=>{r.targetSnapshot=d.targetSnapshot,r.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation.update(h=>(h.finalUrl=d.urlAfterRedirects,h)),this.events.next(new Zi)}),Le(d=>ge(r.routesRecognizeHandler.deferredHandle??q(void 0)).pipe(ne(()=>d))),ht(()=>{let d=new Qi(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:d,extractedUrl:h,source:p,restoredState:g,extras:E}=a,b=new vr(d,this.urlSerializer.serialize(h),p,g);this.events.next(b);let D=gb(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=r=T(m({},a),{targetSnapshot:D,urlAfterRedirects:h,extras:T(m({},E),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(V=>(V.finalUrl=h,V)),q(r)}else return this.events.next(new ln(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Ki.IgnoredByUrlHandlingStrategy)),a.resolve(!1),Me}),ne(a=>{let c=new Dc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=r=T(m({},a),{guards:NT(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),r}),UT(a=>this.events.next(a)),Le(a=>{if(r.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw Fc(this.urlSerializer,a.guardsResult);let c=new Tc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return Me;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",je.GuardRejected),Me;if(a.guards.canActivateChecks.length===0)return q(a);let l=new Mc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return Me;let u=!1;return q(a).pipe(mM(this.paramsInheritanceStrategy),ht({next:()=>{u=!0;let d=new Nc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)},complete:()=>{u||this.cancelNavigationTransition(a,"",je.NoDataFromResolver)}}))}),Yy(a=>{let c=u=>{let d=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let h=u._environmentInjector;d.push(this.configLoader.loadComponent(h,u.routeConfig).then(p=>{u.component=p}))}for(let h of u.children)d.push(...c(h));return d},l=c(a.targetSnapshot.root);return l.length===0?q(a):ge(Promise.all(l).then(()=>a))}),Yy(()=>this.afterPreactivation()),Le(()=>{let{currentSnapshot:a,targetSnapshot:c}=r,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?ge(l).pipe(ne(()=>r)):q(r)}),Ge(1),Le(a=>{let c=xT(n.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=r=a=T(m({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new vo);let l=r.beforeActivateHandler.deferredHandle;return l?ge(l.then(()=>a)):q(a)}),ht(a=>{new qf(n.routeReuseStrategy,r.targetRouterState,r.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(o=!0,this.currentNavigation.update(c=>(c.abort=EM,c)),this.lastSuccessfulNavigation.set(Mn(this.currentNavigation)),this.events.next(new cn(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),Yo(Sb(i.signal).pipe(pt(()=>!o&&!r.targetRouterState),ht(()=>{this.cancelNavigationTransition(r,i.signal.reason+"",je.Aborted)}))),ht({complete:()=>{o=!0}}),Yo(this.transitionAbortWithErrorSubject.pipe(ht(a=>{throw a}))),du(()=>{i.abort(),o||this.cancelNavigationTransition(r,"",je.SupersededByNewNavigation),this.currentTransition?.id===r.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Zo(a=>{if(o=!0,this.destroyed)return r.resolve(!1),Me;if(wb(a))this.events.next(new _t(r.id,this.urlSerializer.serialize(r.extractedUrl),a.message,a.cancellationCode)),MT(a)?this.events.next(new yo(a.url,a.navigationBehaviorOptions)):r.resolve(!1);else{let c=new yr(r.id,this.urlSerializer.serialize(r.extractedUrl),a,r.targetSnapshot??void 0);try{let l=$e(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof _o){let{message:u,cancellationCode:d}=Fc(this.urlSerializer,l);this.events.next(new _t(r.id,this.urlSerializer.serialize(r.extractedUrl),u,d)),this.events.next(new yo(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?r.resolve(!1):r.reject(l)}}return Me}))}))}cancelNavigationTransition(n,r,o){let i=new _t(n.id,this.urlSerializer.serialize(n.extractedUrl),r,o);this.events.next(i),n.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let n=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),r=Mn(this.currentNavigation),o=r?.targetBrowserUrl??r?.extractedUrl;return n.toString()!==o?.toString()&&!r?.extras.skipLocationChange}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();Fb=new k(""),jb=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>y(IM),providedIn:"root"})}return t})(),Uc=class{shouldDetach(e){return!1}store(e,n){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,n){return e.routeConfig===n.routeConfig}shouldDestroyInjector(e){return!0}},IM=(()=>{class t extends Uc{static \u0275fac=(()=>{let n;return function(o){return(n||(n=xi(t)))(o||t)}})();static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),tp=(()=>{class t{urlSerializer=y(ns);options=y(is,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=y(fo);urlHandlingStrategy=y(zc);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Et;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:n,initialUrl:r,targetBrowserUrl:o}){let i=n!==void 0?this.urlHandlingStrategy.merge(n,r):r,s=o??i;return s instanceof Et?this.urlSerializer.serialize(s):s}routerUrlState(n){return n?.targetBrowserUrl===void 0||n?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(n.finalUrl)}}commitTransition({targetRouterState:n,finalUrl:r,initialUrl:o}){r&&n?(this.currentUrlTree=r,this.rawUrlTree=this.urlHandlingStrategy.merge(r,o),this.routerState=n):this.rawUrlTree=o}routerState=gb(null,y(ye));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:()=>y(SM),providedIn:"root"})}return t})(),SM=(()=>{class t extends tp{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(n){return this.location.subscribe(r=>{r.type==="popstate"&&setTimeout(()=>{n(r.url,r.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(n,r){n instanceof vr?this.updateStateMemento():n instanceof ln?this.commitTransition(r):n instanceof Qi?this.urlUpdateStrategy==="eager"&&(r.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(r),r)):n instanceof vo?(this.commitTransition(r),this.urlUpdateStrategy==="deferred"&&!r.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(r),r)):n instanceof _t&&!mb(n)?this.restoreHistory(r):n instanceof yr?this.restoreHistory(r,!0):n instanceof cn&&(this.lastSuccessfulId=n.id,this.currentPageId=this.browserPageId)}setBrowserUrl(n,r){let{extras:o,id:i}=r,{replaceUrl:s,state:a}=o;if(this.location.isCurrentPathEqualTo(n)||s){let c=this.browserPageId,l=m(m({},a),this.generateNgRouterState(i,c,r));this.location.replaceState(n,"",l)}else{let c=m(m({},a),this.generateNgRouterState(i,this.browserPageId+1,r));this.location.go(n,"",c)}}restoreHistory(n,r=!1){if(this.canceledNavigationResolution==="computed"){let o=this.browserPageId,i=this.currentPageId-o;i!==0?this.location.historyGo(i):this.getCurrentUrlTree()===n.finalUrl&&i===0&&(this.resetInternalState(n),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(r&&this.resetInternalState(n),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:n}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,n??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(n,r,o){return this.canceledNavigationResolution==="computed"?m({navigationId:n,\u0275routerPageId:r},this.routerUrlState(o)):m({navigationId:n},this.routerUrlState(o))}static \u0275fac=(()=>{let n;return function(o){return(n||(n=xi(t)))(o||t)}})();static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();qc=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=y(lf);stateManager=y(tp);options=y(is,{optional:!0})||{};pendingTasks=y(xn);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=y(Vb);urlSerializer=y(ns);location=y(fo);urlHandlingStrategy=y(zc);injector=y(ye);_events=new me;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=y(jb);injectorCleanup=y(Fb,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=y(ss,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!y(Hc,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:n=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new De;subscribeToNavigationEvents(){let n=this.navigationTransitions.events.subscribe(r=>{try{let o=this.navigationTransitions.currentTransition,i=Mn(this.navigationTransitions.currentNavigation);if(o!==null&&i!==null){if(this.stateManager.handleRouterEvent(r,i),r instanceof _t&&r.code!==je.Redirect&&r.code!==je.SupersededByNewNavigation)this.navigated=!0;else if(r instanceof cn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(r instanceof yo){let s=r.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(r.url,o.currentRawUrl),c=m({scroll:o.extras.scroll,browserUrl:o.extras.browserUrl,info:o.extras.info,skipLocationChange:o.extras.skipLocationChange,replaceUrl:o.extras.replaceUrl||this.urlUpdateStrategy==="eager"||wM(o.source)},s);this.scheduleNavigation(a,qi,null,c,{resolve:o.resolve,reject:o.reject,promise:o.promise})}}ST(r)&&this._events.next(r)}catch(o){this.navigationTransitions.transitionAbortWithErrorSubject.next(o)}});this.eventsSubscription.add(n)}resetRootComponentType(n){this.routerState.root.component=n,this.navigationTransitions.rootComponentType=n}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),qi,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((n,r,o,i)=>{this.navigateToSyncWithBrowser(n,o,r,i)})}navigateToSyncWithBrowser(n,r,o,i){let s=o?.navigationId?o:null,a=o?.\u0275routerUrl??n;if(o?.\u0275routerUrl&&(i=T(m({},i),{browserUrl:n})),o){let l=m({},o);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(i.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,r,s,i).catch(l=>{this.disposed||this.injector.get(nn)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Mn(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(n){this.config=n.map(Xf),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(n,r={}){let{relativeTo:o,queryParams:i,fragment:s,queryParamsHandling:a,preserveFragment:c}=r,l=c?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=m(m({},this.currentUrlTree.queryParams),i);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=i||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let h=o?o.snapshot:this.routerState.snapshot.root;d=db(h)}catch{(typeof n[0]!="string"||n[0][0]!=="/")&&(n=[]),d=this.currentUrlTree.root}return fb(d,n,u,l??null,this.urlSerializer)}navigateByUrl(n,r={skipLocationChange:!1}){let o=go(n)?n:this.parseUrl(n),i=this.urlHandlingStrategy.merge(o,this.rawUrlTree);return this.scheduleNavigation(i,qi,null,r)}navigate(n,r={skipLocationChange:!1}){return CM(n),this.navigateByUrl(this.createUrlTree(n,r),r)}serializeUrl(n){return this.urlSerializer.serialize(n)}parseUrl(n){try{return this.urlSerializer.parse(n)}catch{return this.console.warn(Xn(4018,!1)),this.urlSerializer.parse("/")}}isActive(n,r){let o;if(r===!0?o=m({},nb):r===!1?o=m({},$f):o=m(m({},$f),r),go(n))return qy(this.currentUrlTree,n,o);let i=this.parseUrl(n);return qy(this.currentUrlTree,i,o)}removeEmptyProps(n){return Object.entries(n).reduce((r,[o,i])=>(i!=null&&(r[o]=i),r),{})}scheduleNavigation(n,r,o,i,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((d,h)=>{a=d,c=h});let u=this.pendingTasks.add();return np(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:r,restoredState:o,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:n,extras:i,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(r){return new(r||t)};static \u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})()});function op(t,...e){return Jt([{provide:ss,multi:!0,useValue:t},[],{provide:An,useFactory:MM},{provide:sc,multi:!0,useFactory:NM},e.map(n=>n.\u0275providers)])}function MM(){return y(qc).routerState.root}function NM(){let t=y(mt);return e=>{let n=t.get(on);if(e!==n.components[0])return;let r=t.get(qc),o=t.get(RM);t.get(kM)===1&&r.initialNavigation(),t.get(AM,null,{optional:!0})?.setUpPreloading(),t.get(TM,null,{optional:!0})?.init(),r.resetRootComponentType(n.componentTypes[0]),o.closed||(o.next(),o.complete(),o.unsubscribe())}}var TM,RM,kM,AM,Ub=f(()=>{"use strict";fe();rp();Kn();TM=new k("");RM=new k("",{factory:()=>new me}),kM=new k("",{factory:()=>1}),AM=new k("")});var ip=f(()=>{"use strict";rp();Ub();});function kt(t){return t instanceof Error?t.message:String(t)}function Hb(t){return t?[...(t.product_recipes||[]).map(e=>({id:e.id,name:e.name})),...(t.base_recipes||[]).map(e=>({id:e.id,name:e.name}))]:[]}function Bb(t){return Object.entries(t?.bar_sizes||{}).filter(([,e])=>e.active!==!1)}var zb=f(()=>{"use strict"});var So,sp=f(()=>{"use strict";fe();zb();fe();So=class t{constructor(){this.state=Fe(null);this.currentCalculation=Fe(null);this.statusMessage=Fe("Starting local workspace...");this.statusIsError=Fe(!1);this.backupPreview=Fe("");this.selectedRecipeId=Fe("");this.selectedBarSize=Fe("");this.volumeIn3=Fe(57.62);this.fragrancePct=Fe(4);this.workspace=uo(()=>this.state()?.workspace??null);this.reports=uo(()=>this.state()?.reports??null);this.worker=null;this.bootstrapped=!1;this.nextId=1;this.pending=new Map}async initialize(){this.bootstrapped||(this.bootstrapped=!0,await this.refresh("seedWorkspace"))}async refresh(e){try{let n=await this.command(e);this.applyState(n,"Local workspace ready."),await this.calculate()}catch(n){this.setStatus(kt(n),!0)}}async calculate(e={}){try{let n={recipeId:e.recipeId??this.selectedRecipeId(),volumeIn3:Number(e.volumeIn3??this.volumeIn3()),barSize:e.barSize??this.selectedBarSize(),fragrancePct:Number(e.fragrancePct??this.fragrancePct())};this.currentCalculation.set(await this.command("calculateBatch",n)),this.setStatus("Batch calculated.")}catch(n){this.setStatus(kt(n),!0)}}async savePlan(){try{this.currentCalculation()||await this.calculate();let e=this.currentCalculation();if(!e)throw new Error("Calculate a batch before saving.");this.applyState(await this.command("savePlannedBatch",{calculation:e}),"Planned batch saved.")}catch(e){this.setStatus(kt(e),!0)}}async runAudit(){try{let e=await this.command("runCapacityAudit",this.batchInput()),n=Object.keys(e.shortages);this.setStatus(n.length?`Short on ${n.join(", ")}.`:"Inventory covers the selected batch.",n.length>0),this.backupPreview.set(JSON.stringify(e,null,2))}catch(e){this.setStatus(kt(e),!0)}}async recordReceipt(e){try{this.applyState(await this.command("recordReceipt",e),"Receipt applied to inventory.")}catch(n){this.setStatus(kt(n),!0)}}async recordAsset(e){try{this.applyState(await this.command("recordAsset",e),"Asset recorded.")}catch(n){this.setStatus(kt(n),!0)}}async handleProductionAction(e,n){try{let r=null;if(e==="next")r=await this.command("recordNextBatchEvent",{batchId:n});else if(e==="cost")r=await this.command("runCosting",{batchId:n});else if(e==="cut"){let o=this.workspace()?.batches?.find(i=>i.id===n);r=await this.command("recordCutYield",{batchId:n,sizeCounts:{[this.selectedBarSize()||"bar_5_2_oz"]:Number(o?.bars_expected||1)},scrapG:0,shreddedG:0})}else r=await this.command("postFinishedGoods",{batchId:n});r&&this.applyState(r,"Production updated.")}catch(r){this.setStatus(kt(r),!0)}}async exportBackup(){try{let e=await this.command("exportBackup"),n=new Blob([e],{type:"application/json"}),r=URL.createObjectURL(n),o=document.createElement("a");o.href=r,o.download=`soap-suite-backup-${new Date().toISOString().slice(0,10)}.json`,o.click(),URL.revokeObjectURL(r),this.backupPreview.set(e),this.setStatus("Backup exported.")}catch(e){this.setStatus(kt(e),!0)}}async importBackup(e){try{this.applyState(await this.command("importBackup",{raw:e}),"Backup imported."),this.backupPreview.set(e)}catch(n){this.setStatus(kt(n),!0)}}async resetDemo(){try{this.currentCalculation.set(null);let e=await this.command("resetDemo");this.applyState(e,"Demo workspace reset."),await this.calculate()}catch(e){this.setStatus(kt(e),!0)}}setStatus(e,n=!1){this.statusMessage.set(e),this.statusIsError.set(n)}batchInput(){return{recipeId:this.selectedRecipeId(),volumeIn3:Number(this.volumeIn3()),barSize:this.selectedBarSize(),fragrancePct:Number(this.fragrancePct())}}applyState(e,n){this.state.set(e),this.syncSelections(e.workspace),this.backupPreview.set(JSON.stringify({workspace:e.workspace,reports:e.reports},null,2)),this.setStatus(n)}syncSelections(e){let n=Hb(e);n.some(i=>i.id===this.selectedRecipeId())||this.selectedRecipeId.set(n[0]?.id||"");let r=Bb(e),o=new Set(r.map(([i])=>i));o.has(this.selectedBarSize())||this.selectedBarSize.set(o.has("bar_5_2_oz")?"bar_5_2_oz":r[0]?.[0]||"")}command(e,n){let r=this.ensureWorker(),o=this.nextId++;return new Promise((i,s)=>{this.pending.set(o,{resolve:i,reject:s}),r.postMessage({id:o,command:e,payload:n??{}})})}ensureWorker(){if(this.worker)return this.worker;if(typeof Worker>"u")throw new Error("Web Workers are not available in this runtime.");return this.worker=new Worker(new URL("worker-YHYXNWMZ.js",import.meta.url),{type:"module"}),this.worker.onmessage=e=>this.handleWorkerMessage(e),this.worker.onerror=e=>this.rejectAll(new Error(e.message||"Workspace worker failed.")),this.worker}handleWorkerMessage(e){let{id:n}=e.data||{},r=this.pending.get(n);r&&(this.pending.delete(n),e.data.ok===!0?r.resolve(e.data.result):r.reject(new Error(e.data.error||"Worker command failed.")))}rejectAll(e){for(let[,n]of this.pending)n.reject(e);this.pending.clear()}static{this.\u0275fac=function(n){return new(n||t)}}static{this.\u0275prov=A({token:t,factory:t.\u0275fac,providedIn:"root"})}}});var Wc,qb=f(()=>{"use strict";fe();ip();sp();fe();Wc=class t{constructor(){this.workspace=y(So);this.workspace.initialize()}static{this.\u0275fac=function(n){return new(n||t)}}static{this.\u0275cmp=pr({type:t,selectors:[["rms-root"]],decls:20,vars:3,consts:[[1,"calculator-shell"],[1,"calculator-header"],["href","https://redmountainsoap.com/",1,"calculator-brand"],["src","./icons/icon.svg","alt","","width","44","height","44"],[1,"calculator-heading"],[1,"calculator-eyebrow"],["aria-live","polite",1,"calculator-status"],[1,"calculator-workspace"]],template:function(n,r){n&1&&(ct(0,"div",0)(1,"header",1)(2,"a",2),hr(3,"img",3),ct(4,"span")(5,"strong"),Tn(6,"Red Mountain Soap"),Nt(),ct(7,"small"),Tn(8,"Recipe tools"),Nt()()(),ct(9,"div",4)(10,"p",5),Tn(11,"Soap Recipe Calculator"),Nt(),ct(12,"h1"),Tn(13,"Design and balance a soap recipe"),Nt(),ct(14,"p"),Tn(15," Adjust oils, formulation settings, and additives while the visual tools show how the recipe changes. "),Nt(),ct(16,"p",6),Tn(17),Nt()()(),ct(18,"main",7),hr(19,"router-outlet"),Nt()()),n&2&&(co(16),lc("warning",r.workspace.statusIsError()),co(),uc(" ",r.workspace.statusMessage()," "))},dependencies:[rs],encapsulation:2})}}});function $M(){let t=y(as);if(!("serviceWorker"in navigator&&t.enabled!==!1))return;let e=y(Gb),n=y(Qe),r=y(on);n.runOutsideAngular(()=>{let o=navigator.serviceWorker,i=()=>o.controller?.postMessage({action:"INITIALIZE"});o.addEventListener("controllerchange",i),r.onDestroy(()=>{o.removeEventListener("controllerchange",i)})}),n.runOutsideAngular(()=>{let o,{registrationStrategy:i}=t;if(typeof i=="function")o=new Promise(s=>i().subscribe(()=>s()));else{let[s,...a]=(i||"registerWhenStable:30000").split(":");switch(s){case"registerImmediately":o=Promise.resolve();break;case"registerWithDelay":o=Wb(+a[0]||0);break;case"registerWhenStable":o=Promise.race([r.whenStable(),Wb(+a[0])]);break;default:throw new x(5600,!1)}}o.then(()=>{r.destroyed||navigator.serviceWorker.register(e,{scope:t.scope,updateViaCache:t.updateViaCache,type:t.type}).catch(s=>console.error(Xn(5604,!1)))})})}function Wb(t){return new Promise(e=>setTimeout(e,t))}function VM(){let t=y(as),e=y(mt),n=!0;return new Co(n&&t.enabled!==!1?navigator.serviceWorker:void 0,e)}function Kb(t,e={}){return Jt([PM,LM,{provide:Gb,useValue:t},{provide:as,useValue:e},{provide:Co,useFactory:VM},ic($M)])}var ap,Co,PM,LM,Gb,as,Qb=f(()=>{"use strict";fe();fe();Kn();Aa();ap="Service workers are disabled or not supported by this browser",Co=class{serviceWorker;worker;registration;events;constructor(e,n){if(this.serviceWorker=e,!e)this.worker=this.events=this.registration=new z(r=>r.error(new x(5601,!1)));else{let r=null,o=new me;this.worker=new z(l=>(r!==null&&l.next(r),o.subscribe(u=>l.next(u))));let i=()=>{let{controller:l}=e;l!==null&&(r=l,o.next(r))};e.addEventListener("controllerchange",i),i(),this.registration=this.worker.pipe(Le(()=>e.getRegistration().then(l=>{if(!l)throw new x(5601,!1);return l})));let s=new me;this.events=s.asObservable();let a=l=>{let{data:u}=l;u?.type&&s.next(u)};e.addEventListener("message",a),n?.get(on,null,{optional:!0})?.onDestroy(()=>{e.removeEventListener("controllerchange",i),e.removeEventListener("message",a)})}}postMessage(e,n){return new Promise(r=>{this.worker.pipe(Ge(1)).subscribe(o=>{o.postMessage(m({action:e},n)),r()})})}postMessageWithOperation(e,n,r){let o=this.waitForOperationCompleted(r),i=this.postMessage(e,n);return Promise.all([i,o]).then(([,s])=>s)}generateNonce(){return Math.round(Math.random()*1e7)}eventsOfType(e){let n;return typeof e=="string"?n=r=>r.type===e:n=r=>e.includes(r.type),this.events.pipe(pt(n))}nextEventOfType(e){return this.eventsOfType(e).pipe(Ge(1))}waitForOperationCompleted(e){return new Promise((n,r)=>{this.eventsOfType("OPERATION_COMPLETED").pipe(pt(o=>o.nonce===e),Ge(1),ne(o=>{if(o.result!==void 0)return o.result;throw new Error(o.error)})).subscribe({next:n,error:r})})}get isEnabled(){return!!this.serviceWorker}},PM=(()=>{class t{sw;messages;notificationClicks;notificationCloses;pushSubscriptionChanges;subscription;get isEnabled(){return this.sw.isEnabled}pushManager=null;subscriptionChanges=new me;constructor(n){if(this.sw=n,!n.isEnabled){this.messages=Kt,this.notificationClicks=Kt,this.notificationCloses=Kt,this.pushSubscriptionChanges=Kt,this.subscription=Kt;return}this.messages=this.sw.eventsOfType("PUSH").pipe(ne(o=>o.data)),this.notificationClicks=this.sw.eventsOfType("NOTIFICATION_CLICK").pipe(ne(o=>o.data)),this.notificationCloses=this.sw.eventsOfType("NOTIFICATION_CLOSE").pipe(ne(o=>o.data)),this.pushSubscriptionChanges=this.sw.eventsOfType("PUSH_SUBSCRIPTION_CHANGE").pipe(ne(o=>o.data)),this.pushManager=this.sw.registration.pipe(ne(o=>o.pushManager));let r=this.pushManager.pipe(Le(o=>o.getSubscription()));this.subscription=new z(o=>{let i=r.subscribe(o),s=this.subscriptionChanges.subscribe(o);return()=>{i.unsubscribe(),s.unsubscribe()}})}requestSubscription(n){if(!this.sw.isEnabled||this.pushManager===null)return Promise.reject(new Error(ap));let r={userVisibleOnly:!0},o=this.decodeBase64(n.serverPublicKey.replace(/_/g,"/").replace(/-/g,"+")),i=new Uint8Array(new ArrayBuffer(o.length));for(let s=0;s<o.length;s++)i[s]=o.charCodeAt(s);return r.applicationServerKey=i,new Promise((s,a)=>{this.pushManager.pipe(Le(c=>c.subscribe(r)),Ge(1)).subscribe({next:c=>{this.subscriptionChanges.next(c),s(c)},error:a})})}unsubscribe(){if(!this.sw.isEnabled)return Promise.reject(new Error(ap));let n=r=>{if(r===null)throw new x(5602,!1);return r.unsubscribe().then(o=>{if(!o)throw new x(5603,!1);this.subscriptionChanges.next(null)})};return new Promise((r,o)=>{this.subscription.pipe(Ge(1),Le(n)).subscribe({next:r,error:o})})}decodeBase64(n){return atob(n)}static \u0275fac=function(r){return new(r||t)(F(Co))};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})(),LM=(()=>{class t{sw;versionUpdates;unrecoverable;get isEnabled(){return this.sw.isEnabled}ongoingCheckForUpdate=null;constructor(n){if(this.sw=n,!n.isEnabled){this.versionUpdates=Kt,this.unrecoverable=Kt;return}this.versionUpdates=this.sw.eventsOfType(["VERSION_DETECTED","VERSION_INSTALLATION_FAILED","VERSION_READY","NO_NEW_VERSION_DETECTED"]),this.unrecoverable=this.sw.eventsOfType("UNRECOVERABLE_STATE")}checkForUpdate(){if(!this.sw.isEnabled)return Promise.reject(new Error(ap));if(this.ongoingCheckForUpdate)return this.ongoingCheckForUpdate;let n=this.sw.generateNonce();return this.ongoingCheckForUpdate=this.sw.postMessageWithOperation("CHECK_FOR_UPDATES",{nonce:n},n).finally(()=>{this.ongoingCheckForUpdate=null}),this.ongoingCheckForUpdate}activateUpdate(){if(!this.sw.isEnabled)return Promise.reject(new x(5601,!1));let n=this.sw.generateNonce();return this.sw.postMessageWithOperation("ACTIVATE_UPDATE",{nonce:n},n)}static \u0275fac=function(r){return new(r||t)(F(Co))};static \u0275prov=A({token:t,factory:t.\u0275fac})}return t})(),Gb=new k("");as=class{enabled;updateViaCache;type;scope;registrationStrategy}});var Gc,Kc,cp,Zb,cs,Yb,Z,Jb,lp,up=f(()=>{"use strict";Gc=globalThis,Kc=Gc.ShadowRoot&&(Gc.ShadyCSS===void 0||Gc.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,cp=Symbol(),Zb=new WeakMap,cs=class{constructor(e,n,r){if(this._$cssResult$=!0,r!==cp)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=n}get styleSheet(){let e=this.o,n=this.t;if(Kc&&e===void 0){let r=n!==void 0&&n.length===1;r&&(e=Zb.get(n)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),r&&Zb.set(n,e))}return e}toString(){return this.cssText}},Yb=t=>new cs(typeof t=="string"?t:t+"",void 0,cp),Z=(t,...e)=>{let n=t.length===1?t[0]:e.reduce((r,o,i)=>r+(s=>{if(s._$cssResult$===!0)return s.cssText;if(typeof s=="number")return s;throw Error("Value passed to 'css' function must be a 'css' function result: "+s+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(o)+t[i+1],t[0]);return new cs(n,t,cp)},Jb=(t,e)=>{if(Kc)t.adoptedStyleSheets=e.map(n=>n instanceof CSSStyleSheet?n:n.styleSheet);else for(let n of e){let r=document.createElement("style"),o=Gc.litNonce;o!==void 0&&r.setAttribute("nonce",o),r.textContent=n.cssText,t.appendChild(r)}},lp=Kc?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let n="";for(let r of e.cssRules)n+=r.cssText;return Yb(n)})(t):t});var FM,jM,UM,HM,BM,zM,Qc,Xb,qM,WM,ls,us,Zc,e_,un,ds=f(()=>{"use strict";up();up();({is:FM,defineProperty:jM,getOwnPropertyDescriptor:UM,getOwnPropertyNames:HM,getOwnPropertySymbols:BM,getPrototypeOf:zM}=Object),Qc=globalThis,Xb=Qc.trustedTypes,qM=Xb?Xb.emptyScript:"",WM=Qc.reactiveElementPolyfillSupport,ls=(t,e)=>t,us={toAttribute(t,e){switch(e){case Boolean:t=t?qM:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let n=t;switch(e){case Boolean:n=t!==null;break;case Number:n=t===null?null:Number(t);break;case Object:case Array:try{n=JSON.parse(t)}catch{n=null}}return n}},Zc=(t,e)=>!FM(t,e),e_={attribute:!0,type:String,converter:us,reflect:!1,useDefault:!1,hasChanged:Zc};Symbol.metadata??=Symbol("metadata"),Qc.litPropertyMetadata??=new WeakMap;un=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,n=e_){if(n.state&&(n.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((n=Object.create(n)).wrapped=!0),this.elementProperties.set(e,n),!n.noAccessor){let r=Symbol(),o=this.getPropertyDescriptor(e,r,n);o!==void 0&&jM(this.prototype,e,o)}}static getPropertyDescriptor(e,n,r){let{get:o,set:i}=UM(this.prototype,e)??{get(){return this[n]},set(s){this[n]=s}};return{get:o,set(s){let a=o?.call(this);i?.call(this,s),this.requestUpdate(e,a,r)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??e_}static _$Ei(){if(this.hasOwnProperty(ls("elementProperties")))return;let e=zM(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(ls("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(ls("properties"))){let n=this.properties,r=[...HM(n),...BM(n)];for(let o of r)this.createProperty(o,n[o])}let e=this[Symbol.metadata];if(e!==null){let n=litPropertyMetadata.get(e);if(n!==void 0)for(let[r,o]of n)this.elementProperties.set(r,o)}this._$Eh=new Map;for(let[n,r]of this.elementProperties){let o=this._$Eu(n,r);o!==void 0&&this._$Eh.set(o,n)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){let n=[];if(Array.isArray(e)){let r=new Set(e.flat(1/0).reverse());for(let o of r)n.unshift(lp(o))}else e!==void 0&&n.push(lp(e));return n}static _$Eu(e,n){let r=n.attribute;return r===!1?void 0:typeof r=="string"?r:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){let e=new Map,n=this.constructor.elementProperties;for(let r of n.keys())this.hasOwnProperty(r)&&(e.set(r,this[r]),delete this[r]);e.size>0&&(this._$Ep=e)}createRenderRoot(){let e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Jb(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,n,r){this._$AK(e,r)}_$ET(e,n){let r=this.constructor.elementProperties.get(e),o=this.constructor._$Eu(e,r);if(o!==void 0&&r.reflect===!0){let i=(r.converter?.toAttribute!==void 0?r.converter:us).toAttribute(n,r.type);this._$Em=e,i==null?this.removeAttribute(o):this.setAttribute(o,i),this._$Em=null}}_$AK(e,n){let r=this.constructor,o=r._$Eh.get(e);if(o!==void 0&&this._$Em!==o){let i=r.getPropertyOptions(o),s=typeof i.converter=="function"?{fromAttribute:i.converter}:i.converter?.fromAttribute!==void 0?i.converter:us;this._$Em=o;let a=s.fromAttribute(n,i.type);this[o]=a??this._$Ej?.get(o)??a,this._$Em=null}}requestUpdate(e,n,r,o=!1,i){if(e!==void 0){let s=this.constructor;if(o===!1&&(i=this[e]),r??=s.getPropertyOptions(e),!((r.hasChanged??Zc)(i,n)||r.useDefault&&r.reflect&&i===this._$Ej?.get(e)&&!this.hasAttribute(s._$Eu(e,r))))return;this.C(e,n,r)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,n,{useDefault:r,reflect:o,wrapped:i},s){r&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,s??n??this[e]),i!==!0||s!==void 0)||(this._$AL.has(e)||(this.hasUpdated||r||(n=void 0),this._$AL.set(e,n)),o===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(n){Promise.reject(n)}let e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(let[o,i]of this._$Ep)this[o]=i;this._$Ep=void 0}let r=this.constructor.elementProperties;if(r.size>0)for(let[o,i]of r){let{wrapped:s}=i,a=this[o];s!==!0||this._$AL.has(o)||a===void 0||this.C(o,void 0,i,a)}}let e=!1,n=this._$AL;try{e=this.shouldUpdate(n),e?(this.willUpdate(n),this._$EO?.forEach(r=>r.hostUpdate?.()),this.update(n)):this._$EM()}catch(r){throw e=!1,this._$EM(),r}e&&this._$AE(n)}willUpdate(e){}_$AE(e){this._$EO?.forEach(n=>n.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(n=>this._$ET(n,this[n])),this._$EM()}updated(e){}firstUpdated(e){}};un.elementStyles=[],un.shadowRootOptions={mode:"open"},un[ls("elementProperties")]=new Map,un[ls("finalized")]=new Map,WM?.({ReactiveElement:un}),(Qc.reactiveElementVersions??=[]).push("2.1.2")});function u_(t,e){if(!mp(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return n_!==void 0?n_.createHTML(e):e}function Ir(t,e,n=t,r){if(e===tt)return e;let o=r!==void 0?n._$Co?.[r]:n._$Cl,i=hs(e)?void 0:e._$litDirective$;return o?.constructor!==i&&(o?._$AO?.(!1),i===void 0?o=void 0:(o=new i(t),o._$AT(t,n,r)),r!==void 0?(n._$Co??=[])[r]=o:n._$Cl=o),o!==void 0&&(e=Ir(t,o._$AS(t,e.values),o,r)),e}var fp,t_,Yc,n_,pp,dn,hp,GM,wr,ps,hs,mp,c_,dp,fs,r_,o_,_r,i_,s_,l_,gp,C,At,Hz,tt,ie,a_,Er,d_,ms,Jc,xo,Sr,Xc,el,tl,nl,f_,KM,p_,Cr=f(()=>{"use strict";fp=globalThis,t_=t=>t,Yc=fp.trustedTypes,n_=Yc?Yc.createPolicy("lit-html",{createHTML:t=>t}):void 0,pp="$lit$",dn=`lit$${Math.random().toFixed(9).slice(2)}$`,hp="?"+dn,GM=`<${hp}>`,wr=document,ps=()=>wr.createComment(""),hs=t=>t===null||typeof t!="object"&&typeof t!="function",mp=Array.isArray,c_=t=>mp(t)||typeof t?.[Symbol.iterator]=="function",dp=`[ 	
\f\r]`,fs=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,r_=/-->/g,o_=/>/g,_r=RegExp(`>|${dp}(?:([^\\s"'>=/]+)(${dp}*=${dp}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),i_=/'/g,s_=/"/g,l_=/^(?:script|style|textarea|title)$/i,gp=t=>(e,...n)=>({_$litType$:t,strings:e,values:n}),C=gp(1),At=gp(2),Hz=gp(3),tt=Symbol.for("lit-noChange"),ie=Symbol.for("lit-nothing"),a_=new WeakMap,Er=wr.createTreeWalker(wr,129);d_=(t,e)=>{let n=t.length-1,r=[],o,i=e===2?"<svg>":e===3?"<math>":"",s=fs;for(let a=0;a<n;a++){let c=t[a],l,u,d=-1,h=0;for(;h<c.length&&(s.lastIndex=h,u=s.exec(c),u!==null);)h=s.lastIndex,s===fs?u[1]==="!--"?s=r_:u[1]!==void 0?s=o_:u[2]!==void 0?(l_.test(u[2])&&(o=RegExp("</"+u[2],"g")),s=_r):u[3]!==void 0&&(s=_r):s===_r?u[0]===">"?(s=o??fs,d=-1):u[1]===void 0?d=-2:(d=s.lastIndex-u[2].length,l=u[1],s=u[3]===void 0?_r:u[3]==='"'?s_:i_):s===s_||s===i_?s=_r:s===r_||s===o_?s=fs:(s=_r,o=void 0);let p=s===_r&&t[a+1].startsWith("/>")?" ":"";i+=s===fs?c+GM:d>=0?(r.push(l),c.slice(0,d)+pp+c.slice(d)+dn+p):c+dn+(d===-2?a:p)}return[u_(t,i+(t[n]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),r]},ms=class t{constructor({strings:e,_$litType$:n},r){let o;this.parts=[];let i=0,s=0,a=e.length-1,c=this.parts,[l,u]=d_(e,n);if(this.el=t.createElement(l,r),Er.currentNode=this.el.content,n===2||n===3){let d=this.el.content.firstChild;d.replaceWith(...d.childNodes)}for(;(o=Er.nextNode())!==null&&c.length<a;){if(o.nodeType===1){if(o.hasAttributes())for(let d of o.getAttributeNames())if(d.endsWith(pp)){let h=u[s++],p=o.getAttribute(d).split(dn),g=/([.?@])?(.*)/.exec(h);c.push({type:1,index:i,name:g[2],strings:p,ctor:g[1]==="."?Xc:g[1]==="?"?el:g[1]==="@"?tl:Sr}),o.removeAttribute(d)}else d.startsWith(dn)&&(c.push({type:6,index:i}),o.removeAttribute(d));if(l_.test(o.tagName)){let d=o.textContent.split(dn),h=d.length-1;if(h>0){o.textContent=Yc?Yc.emptyScript:"";for(let p=0;p<h;p++)o.append(d[p],ps()),Er.nextNode(),c.push({type:2,index:++i});o.append(d[h],ps())}}}else if(o.nodeType===8)if(o.data===hp)c.push({type:2,index:i});else{let d=-1;for(;(d=o.data.indexOf(dn,d+1))!==-1;)c.push({type:7,index:i}),d+=dn.length-1}i++}}static createElement(e,n){let r=wr.createElement("template");return r.innerHTML=e,r}};Jc=class{constructor(e,n){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=n}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){let{el:{content:n},parts:r}=this._$AD,o=(e?.creationScope??wr).importNode(n,!0);Er.currentNode=o;let i=Er.nextNode(),s=0,a=0,c=r[0];for(;c!==void 0;){if(s===c.index){let l;c.type===2?l=new xo(i,i.nextSibling,this,e):c.type===1?l=new c.ctor(i,c.name,c.strings,this,e):c.type===6&&(l=new nl(i,this,e)),this._$AV.push(l),c=r[++a]}s!==c?.index&&(i=Er.nextNode(),s++)}return Er.currentNode=wr,o}p(e){let n=0;for(let r of this._$AV)r!==void 0&&(r.strings!==void 0?(r._$AI(e,r,n),n+=r.strings.length-2):r._$AI(e[n])),n++}},xo=class t{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,n,r,o){this.type=2,this._$AH=ie,this._$AN=void 0,this._$AA=e,this._$AB=n,this._$AM=r,this.options=o,this._$Cv=o?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode,n=this._$AM;return n!==void 0&&e?.nodeType===11&&(e=n.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,n=this){e=Ir(this,e,n),hs(e)?e===ie||e==null||e===""?(this._$AH!==ie&&this._$AR(),this._$AH=ie):e!==this._$AH&&e!==tt&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):c_(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==ie&&hs(this._$AH)?this._$AA.nextSibling.data=e:this.T(wr.createTextNode(e)),this._$AH=e}$(e){let{values:n,_$litType$:r}=e,o=typeof r=="number"?this._$AC(e):(r.el===void 0&&(r.el=ms.createElement(u_(r.h,r.h[0]),this.options)),r);if(this._$AH?._$AD===o)this._$AH.p(n);else{let i=new Jc(o,this),s=i.u(this.options);i.p(n),this.T(s),this._$AH=i}}_$AC(e){let n=a_.get(e.strings);return n===void 0&&a_.set(e.strings,n=new ms(e)),n}k(e){mp(this._$AH)||(this._$AH=[],this._$AR());let n=this._$AH,r,o=0;for(let i of e)o===n.length?n.push(r=new t(this.O(ps()),this.O(ps()),this,this.options)):r=n[o],r._$AI(i),o++;o<n.length&&(this._$AR(r&&r._$AB.nextSibling,o),n.length=o)}_$AR(e=this._$AA.nextSibling,n){for(this._$AP?.(!1,!0,n);e!==this._$AB;){let r=t_(e).nextSibling;t_(e).remove(),e=r}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}},Sr=class{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,n,r,o,i){this.type=1,this._$AH=ie,this._$AN=void 0,this.element=e,this.name=n,this._$AM=o,this.options=i,r.length>2||r[0]!==""||r[1]!==""?(this._$AH=Array(r.length-1).fill(new String),this.strings=r):this._$AH=ie}_$AI(e,n=this,r,o){let i=this.strings,s=!1;if(i===void 0)e=Ir(this,e,n,0),s=!hs(e)||e!==this._$AH&&e!==tt,s&&(this._$AH=e);else{let a=e,c,l;for(e=i[0],c=0;c<i.length-1;c++)l=Ir(this,a[r+c],n,c),l===tt&&(l=this._$AH[c]),s||=!hs(l)||l!==this._$AH[c],l===ie?e=ie:e!==ie&&(e+=(l??"")+i[c+1]),this._$AH[c]=l}s&&!o&&this.j(e)}j(e){e===ie?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}},Xc=class extends Sr{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===ie?void 0:e}},el=class extends Sr{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==ie)}},tl=class extends Sr{constructor(e,n,r,o,i){super(e,n,r,o,i),this.type=5}_$AI(e,n=this){if((e=Ir(this,e,n,0)??ie)===tt)return;let r=this._$AH,o=e===ie&&r!==ie||e.capture!==r.capture||e.once!==r.once||e.passive!==r.passive,i=e!==ie&&(r===ie||o);o&&this.element.removeEventListener(this.name,this,r),i&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}},nl=class{constructor(e,n,r){this.element=e,this.type=6,this._$AN=void 0,this._$AM=n,this.options=r}get _$AU(){return this._$AM._$AU}_$AI(e){Ir(this,e)}},f_={M:pp,P:dn,A:hp,C:1,L:d_,R:Jc,D:c_,V:Ir,I:xo,H:Sr,N:el,U:tl,B:Xc,F:nl},KM=fp.litHtmlPolyfillSupport;KM?.(ms,xo),(fp.litHtmlVersions??=[]).push("3.3.3");p_=(t,e,n)=>{let r=n?.renderBefore??e,o=r._$litPart$;if(o===void 0){let i=n?.renderBefore??null;r._$litPart$=o=new xo(e.insertBefore(ps(),i),i,void 0,n??{})}return o._$AI(t),o}});var vp,zt,QM,h_=f(()=>{"use strict";ds();ds();Cr();Cr();vp=globalThis,zt=class extends un{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){let e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){let n=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=p_(n,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return tt}};zt._$litElement$=!0,zt.finalized=!0,vp.litElementHydrateSupport?.({LitElement:zt});QM=vp.litElementPolyfillSupport;QM?.({LitElement:zt});(vp.litElementVersions??=[]).push("4.2.2")});var m_=f(()=>{"use strict";});var W=f(()=>{"use strict";ds();Cr();h_();m_()});var Do,To,On,rl=f(()=>{"use strict";Do={ATTRIBUTE:1,CHILD:2,PROPERTY:3,BOOLEAN_ATTRIBUTE:4,EVENT:5,ELEMENT:6},To=t=>(...e)=>({_$litDirective$:t,values:e}),On=class{constructor(e){}get _$AU(){return this._$AM._$AU}_$AT(e,n,r){this._$Ct=e,this._$AM=n,this._$Ci=r}_$AS(e,n){return this.update(e,n)}update(e,n){return this.render(...n)}}});var G,g_=f(()=>{"use strict";Cr();rl();G=To(class extends On{constructor(t){if(super(t),t.type!==Do.ATTRIBUTE||t.name!=="class"||t.strings?.length>2)throw Error("`classMap()` can only be used in the `class` attribute and must be the only part in the attribute.")}render(t){return" "+Object.keys(t).filter(e=>t[e]).join(" ")+" "}update(t,[e]){if(this.st===void 0){this.st=new Set,t.strings!==void 0&&(this.nt=new Set(t.strings.join(" ").split(/\s/).filter(r=>r!=="")));for(let r in e)e[r]&&!this.nt?.has(r)&&this.st.add(r);return this.render(e)}let n=t.element.classList;for(let r of this.st)r in e||(n.remove(r),this.st.delete(r));for(let r in e){let o=!!e[r];o===this.st.has(r)||this.nt?.has(r)||(o?(n.add(r),this.st.add(r)):(n.remove(r),this.st.delete(r)))}return tt}})});var wt=f(()=>{"use strict";g_()});var ZM,v_,y_,Mo,Pn,YM,b_,__,ol,E_=f(()=>{"use strict";Cr();({I:ZM}=f_),v_=t=>t,y_=()=>document.createComment(""),Mo=(t,e,n)=>{let r=t._$AA.parentNode,o=e===void 0?t._$AB:e._$AA;if(n===void 0){let i=r.insertBefore(y_(),o),s=r.insertBefore(y_(),o);n=new ZM(i,s,t,t.options)}else{let i=n._$AB.nextSibling,s=n._$AM,a=s!==t;if(a){let c;n._$AQ?.(t),n._$AM=t,n._$AP!==void 0&&(c=t._$AU)!==s._$AU&&n._$AP(c)}if(i!==o||a){let c=n._$AA;for(;c!==i;){let l=v_(c).nextSibling;v_(r).insertBefore(c,o),c=l}}}return n},Pn=(t,e,n=t)=>(t._$AI(e,n),t),YM={},b_=(t,e=YM)=>t._$AH=e,__=t=>t._$AH,ol=t=>{t._$AR(),t._$AA.remove()}});var w_,ue,I_=f(()=>{"use strict";Cr();rl();E_();w_=(t,e,n)=>{let r=new Map;for(let o=e;o<=n;o++)r.set(t[o],o);return r},ue=To(class extends On{constructor(t){if(super(t),t.type!==Do.CHILD)throw Error("repeat() can only be used in text expressions")}dt(t,e,n){let r;n===void 0?n=e:e!==void 0&&(r=e);let o=[],i=[],s=0;for(let a of t)o[s]=r?r(a,s):s,i[s]=n(a,s),s++;return{values:i,keys:o}}render(t,e,n){return this.dt(t,e,n).values}update(t,[e,n,r]){let o=__(t),{values:i,keys:s}=this.dt(e,n,r);if(!Array.isArray(o))return this.ut=s,i;let a=this.ut??=[],c=[],l,u,d=0,h=o.length-1,p=0,g=i.length-1;for(;d<=h&&p<=g;)if(o[d]===null)d++;else if(o[h]===null)h--;else if(a[d]===s[p])c[p]=Pn(o[d],i[p]),d++,p++;else if(a[h]===s[g])c[g]=Pn(o[h],i[g]),h--,g--;else if(a[d]===s[g])c[g]=Pn(o[d],i[g]),Mo(t,c[g+1],o[d]),d++,g--;else if(a[h]===s[p])c[p]=Pn(o[h],i[p]),Mo(t,o[d],o[h]),h--,p++;else if(l===void 0&&(l=w_(s,p,g),u=w_(a,d,h)),l.has(a[d]))if(l.has(a[h])){let E=u.get(s[p]),b=E!==void 0?o[E]:null;if(b===null){let D=Mo(t,o[d]);Pn(D,i[p]),c[p]=D}else c[p]=Pn(b,i[p]),Mo(t,o[d],b),o[E]=null;p++}else ol(o[h]),h--;else ol(o[d]),d++;for(;p<=g;){let E=Mo(t,c[g+1]);Pn(E,i[p]),c[p++]=E}for(;d<=h;){let E=o[d++];E!==null&&ol(E)}return this.ut=s,b_(t,c),tt}})});var Ot=f(()=>{"use strict";I_()});var S_=f(()=>{"use strict";});function _(t){return(e,n)=>typeof n=="object"?XM(t,e,n):((r,o,i)=>{let s=o.hasOwnProperty(i);return o.constructor.createProperty(i,r),s?Object.getOwnPropertyDescriptor(o,i):void 0})(t,e,n)}var JM,XM,yp=f(()=>{"use strict";ds();JM={attribute:!0,type:String,converter:us,reflect:!1,hasChanged:Zc},XM=(t=JM,e,n)=>{let{kind:r,metadata:o}=n,i=globalThis.litPropertyMetadata.get(o);if(i===void 0&&globalThis.litPropertyMetadata.set(o,i=new Map),r==="setter"&&((t=Object.create(t)).wrapped=!0),i.set(n.name,t),r==="accessor"){let{name:s}=n;return{set(a){let c=e.get.call(this);e.set.call(this,a),this.requestUpdate(s,c,t,!0,a)},init(a){return a!==void 0&&this.C(s,void 0,t,a),a}}}if(r==="setter"){let{name:s}=n;return function(a){let c=this[s];e.call(this,a),this.requestUpdate(s,c,t,!0,a)}}throw Error("Unsupported decorator location: "+r)}});function No(t){return _(T(m({},t),{state:!0,attribute:!1}))}var C_=f(()=>{"use strict";yp();});var x_=f(()=>{"use strict";});var xr,Ro=f(()=>{"use strict";xr=(t,e,n)=>(n.configurable=!0,n.enumerable=!0,Reflect.decorate&&typeof e!="object"&&Object.defineProperty(t,e,n),n)});function fn(t,e){return(n,r,o)=>{let i=s=>s.renderRoot?.querySelector(t)??null;if(e){let{get:s,set:a}=typeof r=="object"?n:o??(()=>{let c=Symbol();return{get(){return this[c]},set(l){this[c]=l}}})();return xr(n,r,{get(){let c=s.call(this);return c===void 0&&(c=i(this),(c!==null||this.hasUpdated)&&a.call(this,c)),c}})}return xr(n,r,{get(){return i(this)}})}}var D_=f(()=>{"use strict";Ro();});var T_=f(()=>{"use strict";Ro();});var M_=f(()=>{"use strict";Ro();});var N_=f(()=>{"use strict";Ro();});var R_=f(()=>{"use strict";Ro();});var nt=f(()=>{"use strict";S_();yp();C_();x_();D_();T_();M_();N_();R_()});var k_,A_=f(()=>{"use strict";W();k_=Z`:host {
  display: block;
}

.feedback {
  display: grid;
  gap: 6px;
}

button,
.quiet {
  min-height: 32px;
  border-radius: 8px;
  padding: 7px 10px;
  font: inherit;
  text-align: left;
}

button {
  border: 1px solid var(--_vector-line);
  background: var(--_vector-surface);
  color: var(--_vector-ink);
  cursor: pointer;
}

button.hard {
  border-color: color-mix(in srgb, var(--_vector-danger) 45%, transparent);
  color: var(--_vector-danger);
  background: color-mix(in srgb, var(--_vector-danger) 8%, transparent);
}

button.soft {
  border-color: color-mix(in srgb, var(--_vector-warning) 45%, transparent);
  color: var(--_vector-warning);
  background: color-mix(in srgb, var(--_vector-warning) 10%, transparent);
}

.quiet {
  display: block;
  color: var(--_vector-muted);
  border: 1px dashed var(--_vector-line);
}`});var il,_e,gs,O_,qt=f(()=>{"use strict";il=Object.freeze({ink:"#1d2523",inkSoft:"#38413d",muted:"#68716d",line:"#d9d5ca",surface:"#fffdfa",surfaceAlt:"#fbfaf6",surfaceStrong:"#f0eee8",track:"#e8e3d8",allowed:"#b9cfc6",accent:"#426a8c",accentStrong:"#274c43",accentSoft:"#dce8e2",preview:"#d9824b",ok:"#4f8f5b",danger:"#9b3b37",warning:"#d9824b",reach:"#68716d"}),_e=Object.freeze(["#426a8c","#d9824b","#6f8d42","#8b5f9d","#9d9a5a","#c48a70"]),gs=String.fromCodePoint(128274),O_=String.fromCodePoint(128275)});var se,P_=f(()=>{"use strict";W();se=Z`:host {
  --_vector-ink: var(--vector-ink, #1d2523);
  --_vector-ink-soft: var(--vector-ink-soft, #38413d);
  --_vector-muted: var(--vector-muted, #68716d);
  --_vector-line: var(--vector-line, #d9d5ca);
  --_vector-surface: var(--vector-surface, #fffdfa);
  --_vector-surface-alt: var(--vector-surface-alt, #fbfaf6);
  --_vector-surface-strong: var(--vector-surface-strong, #f0eee8);
  --_vector-track: var(--vector-track, #e8e3d8);
  --_vector-allowed: var(--vector-allowed, #b9cfc6);
  --_vector-accent: var(--vector-accent, #426a8c);
  --_vector-accent-strong: var(--vector-accent-strong, #274c43);
  --_vector-accent-soft: var(--vector-accent-soft, #dce8e2);
  --_vector-preview: var(--vector-preview, #d9824b);
  --_vector-ok: var(--vector-ok, #4f8f5b);
  --_vector-danger: var(--vector-danger, #9b3b37);
  --_vector-warning: var(--vector-warning, #d9824b);
  --_vector-reach: var(--vector-reach, #68716d);
}`});function eN(t){return String(t??"").replace(/[<>"'&;]/g,"")}function ko(t,e={},n={}){for(let r of Object.keys(n)){if(r in e)continue;let o=r.replace(/[A-Z]/g,i=>`-${i.toLowerCase()}`);t.style.removeProperty(`--vector-${o}`)}for(let[r,o]of Object.entries(e??{})){let i=r.replace(/[A-Z]/g,s=>`-${s.toLowerCase()}`);t.style.setProperty(`--vector-${i}`,eN(o))}}var Ue=f(()=>{"use strict";qt();P_()});function ke(t,e){globalThis.customElements&&!customElements.get(t)&&customElements.define(t,e)}var te,He=f(()=>{"use strict";W();Ue();te=class extends zt{constructor(){super(...arguments);this._theme={}}get theme(){return this._theme}set theme(n){let r=this._theme;this._theme=n??{},ko(this,this._theme,r)}}});function P(t="tx"){return`${t}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2,8)}`}function w(t,e=4){let n=10**e;return Math.round((Number(t??0)+Number.EPSILON)*n)/n}function Ce(t,e=.1){let n=Number(e??.1);return w(Math.round(t/n)*n,tN(n))}function K(t,e=-1/0,n=1/0){return Math.min(n,Math.max(e,Number(t??0)))}function Q(t){return t.replaceAll("_"," ").replace(/\b\w/g,e=>e.toUpperCase())}function tN(t){let e=String(t);return e.includes(".")?e.split(".")[1].length:0}var bp=f(()=>{"use strict"});function O(t,e,n){t.dispatchEvent(new CustomEvent(e,{bubbles:!0,composed:!0,detail:n}))}var L_=f(()=>{"use strict"});function pe(t={}){return Object.fromEntries(Object.entries(t).map(([e,n])=>[e,Number(n??0)]))}function sN(t=[],e={}){let n=[...t].sort((r,o)=>Number(r.order??0)-Number(o.order??0)).map(r=>r.key);return[...new Set([...n,...Object.keys(e)])]}function Ln(t={}){return Object.values(t).reduce((e,n)=>e+Number(n??0),0)}function al(t={}){return Number.isFinite(Number(t.min))?Number(t.min):0}function cl(t={}){return Number.isFinite(Number(t.max))?Number(t.max):Dr}function he(t,e,n,r="hard"){return{key:t,type:e,severity:r,message:n}}function Ep(t={},e=Dr,n=Object.keys(t)){let r={},o=Ln(t),i=n.length?Number(e??Dr)/n.length:0;for(let s of n)r[s]=o>0?Number(t[s]??0)*Number(e??Dr)/o:i;return Tr(r,{},e,n[0])}function Mr({value:t={},key:e,target:n,constraints:r={},items:o=[],total:i=Dr}){let s=sN(o,t).filter(I=>I in t||I===e),a=[],c=Ep(t,i,s),l=pe(c),u=r[e]||{};if(!e||!(e in l))return{vector:Tr(l,r,i,e),hits:[he(e||"composition","sum","No active slice was available.")],delta:0};if(u.locked)return{vector:l,hits:[he(e,"locked",`${Q(e)} is locked.`)],delta:0};let d=Number(n),h=al(u),p=cl(u),g=K(d,h,p);g!==d&&a.push(he(e,g<=h?"min":"max",`${Q(e)} hit ${g<=h?"minimum":"maximum"} ${w(g,2)}%.`)),g=Ce(g,u.step??j_);let E=g-l[e];l[e]=g;let b=-E,D=new Set([e]),V=0;for(;H_(b)&&V<cN(s);){V+=1;let I=s.filter(j=>{if(D.has(j))return!1;let Ze=r[j]||{};return Ze.locked?(a.push(he(j,"locked",`${Q(j)} is locked.`)),D.add(j),!1):b<0?dN(l[j],Ze):fN(l[j],Ze)});if(!I.length){F_(l,e,b),a.push(he(e,"unreachable",`${Q(e)} was clamped because the remaining oils cannot absorb the change.`)),b=0;break}let le=I.reduce((j,Ze)=>j+V_(l[Ze]),0),ae=!1;for(let j of I){let Ze=r[j]||{},kr=V_(l[j])/le,Lt=b*kr,$t=al(Ze),Gt=cl(Ze),S=Lt;Lt<0&&l[j]+Lt<$t?(S=$t-l[j],l[j]=$t,D.add(j),a.push(he(j,"min",`${Q(j)} hit minimum ${w($t,2)}%.`))):Lt>0&&l[j]+Lt>Gt?(S=Gt-l[j],l[j]=Gt,D.add(j),a.push(he(j,"max",`${Q(j)} hit maximum ${w(Gt,2)}%.`))):l[j]+=S,b=pN(b-S),ae=ae||uN(S)}if(!ae){F_(l,e,b),a.push(he(e,"unreachable","No remaining oil can legally move.")),b=0;break}}return{vector:Tr(ll(l,r),r,i,e),hits:aN(a),delta:w(g-c[e],4)}}function ll(t={},e={}){return Object.fromEntries(Object.entries(t).map(([n,r])=>[n,Ce(r,e[n]?.step??j_)]))}function Tr(t={},e={},n=Dr,r=null){let o=pe(t),i=Object.keys(o),s=sl(Number(n??Dr)-Ln(o)),a=0;for(;H_(s)&&a<iN;){a+=1;let c=[r,...i.filter(u=>u!==r)].filter(u=>!!u),l=!1;for(let u of c){let d=e[u]||{};if(d.locked)continue;let h=al(d),p=cl(d),g=s>0?p-o[u]:o[u]-h;if(!lN(g))continue;let E=Math.sign(s)*Math.min(Math.abs(s),g);o[u]=sl(o[u]+E),s=sl(s-E),l=!0;break}if(!l)break}return o}function ul(t={},e={}){return[...new Set([...Object.keys(t),...Object.keys(e)])].reduce((r,o)=>r+Math.abs(Number(t[o]??0)-Number(e[o]??0)),0)}function aN(t=[]){let e=new Set;return t.filter(n=>{let r=`${n.key}:${n.type}:${n.message}`;return e.has(r)?!1:(e.add(r),!0)})}function cN(t){return t.length+oN}function H_(t){return Math.abs(Number(t??0))>_p}function lN(t){return Number(t??0)>U_}function uN(t){return Math.abs(Number(t??0))>U_}function dN(t,e){return Number(t??0)>al(e)+_p}function fN(t,e){return Number(t??0)<cl(e)-_p}function V_(t){return Math.max($_,Number(t??$_))}function sl(t){return w(t,nN)}function pN(t){return w(t,rN)}function F_(t,e,n){t[e]=sl(Number(t[e]??0)+Number(n??0))}var Dr,j_,nN,rN,_p,U_,$_,oN,iN,xe=f(()=>{"use strict";bp();L_();bp();Dr=100,j_=.1,nN=4,rN=7,_p=1e-7,U_=1e-9,$_=1e-4,oN=8,iN=100});var vs,wp=f(()=>{"use strict";W();nt();Ot();A_();He();xe();Ue();vs=class extends te{static{this.styles=[se,k_]}#e;get hits(){return this.#e}set hits(e){this.#e=e}constructor(){super(),this.hits=[],this.theme={}}render(){return C`
      <div class="feedback">
        ${this.hits.length?ue(this.hits,e=>`${e.key}:${e.type}:${e.message}`,e=>C`
                <button
                  class=${e.severity||"hard"}
                  data-key=${e.key}
                  @click=${()=>this._selectHit(e.key)}
                >
                  ${e.message}
                </button>
              `):C`<span class="quiet">No active constraints.</span>`}
      </div>
    `}_selectHit(e){O(this,"vector-select",{source:"constraint",role:"selection",transactionId:P("feedback"),key:e})}};v([_({attribute:!1})],vs.prototype,"hits",1)});var B_=f(()=>{"use strict";rl()});var Ip,de,pn=f(()=>{"use strict";W();B_();Ip=class extends On{constructor(n){super(n);this.previousNames=new Set;if(n.type!==Do.ELEMENT)throw new Error("styleVariables must be used as an element directive")}render(n){return tt}update(n,[r]){let o=n.element,i=new Set(Object.keys(r));for(let s of this.previousNames)i.has(s)||o.style.removeProperty(s);for(let[s,a]of Object.entries(r))a===null?o.style.removeProperty(s):o.style.setProperty(s,a);return this.previousNames=i,tt}},de=To(Ip)});var z_,q_=f(()=>{"use strict";W();z_=Z`:host {
  display: block;
}

.search-list {
  display: grid;
  gap: 10px;
}

.search {
  min-height: 36px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  padding: 0 10px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
}

.items {
  display: grid;
  gap: 6px;
  max-block-size: 280px;
  overflow: auto;
}

.oil-row {
  display: grid;
  grid-template-columns: 14px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  min-height: 34px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
  text-align: left;
  cursor: pointer;
}

.oil-row.selected {
  border-color: var(--_vector-accent-strong);
  background: var(--_vector-accent-soft);
}

.swatch {
  inline-size: 12px;
  block-size: 12px;
  border-radius: 999px;
  background: var(--item-color);
}

.item-label {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

small {
  display: block;
  color: var(--_vector-muted);
  font-size: 0.7rem;
  text-transform: capitalize;
}

strong {
  color: var(--_vector-accent-strong);
  font-size: 0.78rem;
}`});function Sp(t,e=_e){let n=ys(e);return n[t%n.length]}function dt(t,e=0,n=_e){return t?.color??Sp(e,n)}function W_(t,e=[]){return e.find(n=>Number(t)>=Number(n.min)&&Number(t)<=Number(n.max))||null}function ys(t){return Array.isArray(t)&&t.length?t:_e}function Ao(t=1){let e=Number(t??1);if(!Number.isFinite(e)||e>=1)return 0;let n=String(e);return n.includes("e-")?Number(n.split("e-")[1]||0):n.includes(".")?n.split(".")[1].length:0}function Be(t,e=0){let n=Math.max(0,Number(e??0));return n>0?Number(t||0).toFixed(n):String(w(t,0))}function Cp(t,e=""){return String(t?.symbol??e??"").trim()||t?.label||""}var hn=f(()=>{"use strict";qt();xe()});var G_,K_,Q_,Z_,Y_,J_,X_,eE,ze,bs=f(()=>{"use strict";W();nt();wt();Ot();pn();q_();He();qt();hn();xe();Ue();ze=class extends te{constructor(){super();N(this,G_);N(this,K_);N(this,Q_);N(this,Z_);N(this,Y_);N(this,J_);N(this,X_);N(this,eE);this._queryChanged=n=>{this.query=n.currentTarget.value};this.items=[],this.selectedKeys=[],this.query="",this.activeTags=[],this.itemTypes=[],this.listType="item",this.searchPlaceholder="Search items",this.theme={},this.palette=_e}static{this.styles=[se,z_]}get items(){return M(this,G_)}set items(n){R(this,G_,n)}get selectedKeys(){return M(this,K_)}set selectedKeys(n){R(this,K_,n)}get query(){return M(this,Q_)}set query(n){R(this,Q_,n)}get activeTags(){return M(this,Z_)}set activeTags(n){R(this,Z_,n)}get itemTypes(){return M(this,Y_)}set itemTypes(n){R(this,Y_,n)}get listType(){return M(this,J_)}set listType(n){R(this,J_,n)}get searchPlaceholder(){return M(this,X_)}set searchPlaceholder(n){R(this,X_,n)}get palette(){return M(this,eE)}set palette(n){R(this,eE,n)}set state(n){this.items=n?.items??[],this.selectedKeys=n?.selectedKeys??[],this.query=n?.query??"",this.activeTags=n?.activeTags??[],this.itemTypes=n?.itemTypes??[],this.listType=n?.listType??"item",this.searchPlaceholder=n?.searchPlaceholder??"Search items",this.theme=n?.theme??{},this.palette=n?.palette??_e}render(){let n=new Set(this.selectedKeys),r=this.query.toLowerCase(),o=this.items.filter(i=>{let s=!r||i.label.toLowerCase().includes(r)||i.key.toLowerCase().includes(r),a=!this.activeTags.length||this.activeTags.every(u=>i.tags?.includes(u)),c=i.type||i.category||"",l=!this.itemTypes.length||this.itemTypes.includes(c);return s&&a&&l});return C`
      <div class="search-list">
        <input
          class="search"
          type="search"
          placeholder=${this.searchPlaceholder}
          .value=${this.query}
          @input=${this._queryChanged}
        />
        <div class="items">
          ${ue(o,i=>i.key,(i,s)=>C`
              <button
                class=${G({"oil-row":!0,selected:n.has(i.key)})}
                data-key=${i.key}
                @click=${()=>this._toggle(i.key)}
              >
                <span
                  class="swatch"
                  ${de({"--item-color":dt(i,s,this.palette)})}
                ></span>
                <span
                  ><span class="item-label">${i.label}</span
                  >${i.category||i.type?C`<small>${i.category||i.type}</small>`:ie}</span
                >
                <strong>${n.has(i.key)?"Remove":"Add"}</strong>
              </button>
            `)}
        </div>
      </div>
    `}_toggle(n){let r=new Set(this.selectedKeys),o=r.has(n)?"remove":"add";r.has(n)?r.delete(n):r.add(n),O(this,"vector-change",{source:"list",role:"selection",transactionId:P("list"),key:n,selection:[...r],action:o,listType:this.listType,itemType:this.items.find(i=>i.key===n)?.type??this.items.find(i=>i.key===n)?.category})}};G_=new WeakMap,K_=new WeakMap,Q_=new WeakMap,Z_=new WeakMap,Y_=new WeakMap,J_=new WeakMap,X_=new WeakMap,eE=new WeakMap,v([_({attribute:!1})],ze.prototype,"items",1),v([_({attribute:!1})],ze.prototype,"selectedKeys",1),v([_({attribute:!1})],ze.prototype,"query",1),v([_({attribute:!1})],ze.prototype,"activeTags",1),v([_({attribute:!1})],ze.prototype,"itemTypes",1),v([_({attribute:!1})],ze.prototype,"listType",1),v([_({attribute:!1})],ze.prototype,"searchPlaceholder",1),v([_({attribute:!1})],ze.prototype,"palette",1)});var dl,xp=f(()=>{"use strict";bs();dl=class extends ze{constructor(){super(),this._listType="oil",this._searchPlaceholder="Search oils"}}});var tE,nE=f(()=>{"use strict";W();tE=Z`:host {
  display: block;
}

.amount-list {
  display: grid;
  gap: 8px;
}

.quiet {
  min-height: 34px;
  border: 1px dashed var(--_vector-line);
  border-radius: 8px;
  color: var(--_vector-muted);
  padding: 8px 10px;
}`});var Ee,Dp=f(()=>{"use strict";W();nt();Ot();nE();He();qt();Ue();Ee=class extends te{static{this.styles=[se,tE]}#e;get items(){return this.#e}set items(e){this.#e=e}#t;get values(){return this.#t}set values(e){this.#t=e}#n;get constraints(){return this.#n}set constraints(e){this.#n=e}#r;get thresholds(){return this.#r}set thresholds(e){this.#r=e}#o;get unitPreferences(){return this.#o}set unitPreferences(e){this.#o=e}#i;get measurementSystem(){return this.#i}set measurementSystem(e){this.#i=e}#s;get basis(){return this.#s}set basis(e){this.#s=e}#a;get measurementScope(){return this.#a}set measurementScope(e){this.#a=e}#c;get measurementMeasure(){return this.#c}set measurementMeasure(e){this.#c=e}#l;get context(){return this.#l}set context(e){this.#l=e}#u;get role(){return this.#u}set role(e){this.#u=e}#d;get control(){return this.#d}set control(e){this.#d=e}#f;get unitDisplay(){return this.#f}set unitDisplay(e){this.#f=e}#p;get activeEmphasis(){return this.#p}set activeEmphasis(e){this.#p=e}#h;get palette(){return this.#h}set palette(e){this.#h=e}constructor(){super(),this.items=[],this.values={},this.constraints={},this.thresholds={},this.unitPreferences={},this.measurementSystem="",this.basis="",this.measurementScope="",this.measurementMeasure="",this.context=null,this.role="additive",this.control="slider",this.unitDisplay="picker",this.activeEmphasis=!0,this.theme={},this.palette=_e}set state(e){let n=e??{};this.items=n.items??[],this.values=n.values??{},this.constraints=n.constraints??{},this.thresholds=n.thresholds??{},this.unitPreferences=n.unitPreferences??{},this.measurementSystem=n.measurementSystem??"",this.basis=n.basis??"",this.measurementScope=n.measurementScope??"",this.measurementMeasure=n.measurementMeasure??"",this.context=n.context??null,this.role=n.role??"additive",this.control=n.control??"slider",this.unitDisplay=n.unitDisplay??"picker",this.activeEmphasis=n.activeEmphasis!==!1,this.theme=n.theme??{},this.palette=n.palette??_e}render(){return C`
      <div class="amount-list">
        ${this.items.length?ue(this.items,e=>e.key,e=>{let n={item:e,value:this.values[e.key]??e.defaultValue??0,defaultValue:e.defaultValue,constraint:this.constraints[e.key]??{},hits:[],thresholds:this.thresholds[e.key]??e.thresholds??[],unitOptions:e.unitOptions??[],unitKey:this._unitKeyForItem(e),measurementSystem:this.measurementSystem,basis:this.basis,measurementScope:this.measurementScope,measurementMeasure:this.measurementMeasure,context:T(m({},this.context??{}),{density:this.context?.densities?.[e.material||e.key]??this.context?.density}),role:this.role,rangeMin:e.min??0,rangeMax:e.max??100,precision:e.precision,theme:this.theme,palette:this.palette,variant:"embedded",unitDisplay:this.unitDisplay,activeEmphasis:this.activeEmphasis};return this._renderControl(n)}):C`<div class="quiet">No selected amounts.</div>`}
      </div>
    `}_renderControl(e){return this.control==="constraint"?C`<vector-scalar-rail
          .item=${e.item}
          .value=${e.value}
          .constraint=${e.constraint}
          .role=${e.role}
          .hits=${e.hits}
          .rangeMin=${e.rangeMin}
          .rangeMax=${e.rangeMax}
          .precision=${e.precision??null}
          .defaultValue=${e.defaultValue??null}
          .thresholds=${e.thresholds??[]}
          .unitOptions=${e.unitOptions??[]}
          .unitKey=${e.unitKey??""}
          .measurementSystem=${e.measurementSystem??""}
          .basis=${e.basis??""}
          .measurementScope=${e.measurementScope??""}
          .measurementMeasure=${e.measurementMeasure??""}
          .context=${e.context??null}
          .disabled=${e.disabled}
          .palette=${e.palette}
          .variant=${e.variant}
          .unitDisplay=${e.unitDisplay}
          .activeEmphasis=${e.activeEmphasis}
        ></vector-scalar-rail>`:C`<vector-value-slider
          .item=${e.item}
          .value=${e.value}
          .constraint=${e.constraint}
          .role=${e.role}
          .hits=${e.hits}
          .rangeMin=${e.rangeMin}
          .rangeMax=${e.rangeMax}
          .precision=${e.precision??null}
          .defaultValue=${e.defaultValue??null}
          .thresholds=${e.thresholds??[]}
          .unitOptions=${e.unitOptions??[]}
          .unitKey=${e.unitKey??""}
          .measurementSystem=${e.measurementSystem??""}
          .basis=${e.basis??""}
          .measurementScope=${e.measurementScope??""}
          .measurementMeasure=${e.measurementMeasure??""}
          .context=${e.context??null}
          .disabled=${e.disabled}
          .palette=${e.palette}
          .variant=${e.variant}
          .unitDisplay=${e.unitDisplay}
          .activeEmphasis=${e.activeEmphasis}
        ></vector-value-slider>`}_unitKeyForItem(e){let n=this.unitPreferences[e.key]??"";return!n||!(e.unitOptions??[]).find(o=>o.key===n)?"":n}};v([_({attribute:!1})],Ee.prototype,"items",1),v([_({attribute:!1})],Ee.prototype,"values",1),v([_({attribute:!1})],Ee.prototype,"constraints",1),v([_({attribute:!1})],Ee.prototype,"thresholds",1),v([_({attribute:!1})],Ee.prototype,"unitPreferences",1),v([_({attribute:!1})],Ee.prototype,"measurementSystem",1),v([_({attribute:!1})],Ee.prototype,"basis",1),v([_({attribute:!1})],Ee.prototype,"measurementScope",1),v([_({attribute:!1})],Ee.prototype,"measurementMeasure",1),v([_({attribute:!1})],Ee.prototype,"context",1),v([_({attribute:!1})],Ee.prototype,"role",1),v([_({attribute:!1})],Ee.prototype,"control",1),v([_({attribute:!1})],Ee.prototype,"unitDisplay",1),v([_({attribute:!1})],Ee.prototype,"activeEmphasis",1),v([_({attribute:!1})],Ee.prototype,"palette",1)});var rE,oE=f(()=>{"use strict";W();rE=Z`:host {
  display: block;
}

.choice-control {
  display: grid;
  gap: 8px;
}

strong {
  color: var(--_vector-ink);
  font-size: 0.85rem;
}

.choices {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

button {
  min-height: 32px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
  padding: 0 10px;
  cursor: pointer;
}

button.selected {
  border-color: var(--_vector-accent-strong);
  background: var(--_vector-accent-soft);
  color: var(--_vector-accent-strong);
  font-weight: 700;
}

button:disabled {
  opacity: 0.58;
  cursor: default;
}`});var mn,Tp=f(()=>{"use strict";W();nt();wt();Ot();oE();He();xe();Ue();mn=class extends te{static{this.styles=[se,rE]}#e;get item(){return this.#e}set item(e){this.#e=e}#t;get options(){return this.#t}set options(e){this.#t=e}#n;get value(){return this.#n}set value(e){this.#n=e}#r;get role(){return this.#r}set role(e){this.#r=e}#o;get disabled(){return this.#o}set disabled(e){this.#o=e}constructor(){super(),this.item=null,this.options=[],this.value="",this.role="choice",this.disabled=!1,this.theme={}}set state(e){let n=e??{};this.item=n.item??null,this.options=n.options??n.item?.options??[],this.value=n.value??"",this.role=n.role??"choice",this.disabled=n.disabled??!1,this.theme=n.theme??{}}render(){return C`
      <div class="choice-control">
        ${this.item?C`<strong>${this.item.label||Q(this.item.key)}</strong>`:ie}
        <div
          class="choices"
          role="group"
          aria-label=${this.item?.label||"Choice"}
        >
          ${ue(this.options,e=>e.key??e.value??"",e=>{let n=e.value??e.key??"",r=n===this.value;return C`
                <button
                  type="button"
                  class=${G({selected:r})}
                  ?disabled=${this.disabled}
                  aria-pressed=${String(!!r)}
                  @click=${()=>this._select(n,e)}
                >
                  ${e.label??Q(String(n))}
                </button>
              `})}
        </div>
      </div>
    `}_select(e,n){O(this,"vector-change",{source:"inspector",role:this.role,transactionId:P("choice"),key:this.item?.key,value:e,option:n})}};v([_({attribute:!1})],mn.prototype,"item",1),v([_({attribute:!1})],mn.prototype,"options",1),v([_({attribute:!1})],mn.prototype,"value",1),v([_({attribute:!1})],mn.prototype,"role",1),v([_({attribute:!1})],mn.prototype,"disabled",1)});var iE,sE=f(()=>{"use strict";W();iE=Z`:host {
  display: block;
}

.constraint-row {
  display: grid;
  grid-template-columns: minmax(90px, 1fr) 56px repeat(3, minmax(60px, 0.7fr));
  gap: 8px;
  align-items: end;
  padding: 8px 0;
  border-bottom: 1px solid var(--_vector-line);
}

strong {
  color: var(--_vector-ink);
  font-size: 0.88rem;
}

span,
label {
  color: var(--_vector-muted);
  font-size: 0.78rem;
}

input {
  min-height: 30px;
  width: 100%;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  padding: 0 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
}

input[type=checkbox] {
  inline-size: 20px;
}

@media (max-width: 720px) {
  .constraint-row {
    grid-template-columns: 1fr 56px;
  }
  label {
    display: grid;
    grid-template-columns: 48px 1fr;
    grid-column: 1/-1;
    align-items: center;
  }
}`});var aE,cE,lE,uE,$n,Mp=f(()=>{"use strict";W();nt();sE();He();xe();Ue();$n=class extends te{constructor(){super();N(this,aE);N(this,cE);N(this,lE);N(this,uE);this._emit=n=>{let r=n.currentTarget,o=new FormData(r),i={min:Number(o.get("min")),max:Number(o.get("max")),locked:o.get("locked")==="on"};O(this,"vector-change",{source:"constraint",role:"constraint",transactionId:P("constraint"),key:this.item?.key,constraintRole:this.mode,constraint:i})};this.item=null,this.value=0,this.constraint={},this.mode="composition",this.theme={}}static{this.styles=[se,iE]}get item(){return M(this,aE)}set item(n){R(this,aE,n)}get value(){return M(this,cE)}set value(n){R(this,cE,n)}get constraint(){return M(this,lE)}set constraint(n){R(this,lE,n)}get mode(){return M(this,uE)}set mode(n){R(this,uE,n)}render(){let n=this.item||{key:"",label:"Constraint"};return C`
      <form class="constraint-row" @change=${this._emit}>
        <strong>${n.label||Q(n.key)}</strong>
        <span
          >${w(this.value,1)}${this.mode==="composition"?"%":""}</span
        >
        <label
          >Min<input
            name="min"
            data-field="min"
            type="number"
            step=${this.constraint.step||.1}
            .value=${String(this.constraint.min??0)}
        /></label>
        <label
          >Max<input
            name="max"
            data-field="max"
            type="number"
            step=${this.constraint.step||.1}
            .value=${String(this.constraint.max??100)}
        /></label>
        <label
          >Lock<input
            name="locked"
            data-field="locked"
            type="checkbox"
            .checked=${!!this.constraint.locked}
        /></label>
      </form>
    `}};aE=new WeakMap,cE=new WeakMap,lE=new WeakMap,uE=new WeakMap,v([_({attribute:!1})],$n.prototype,"item",1),v([_({attribute:!1})],$n.prototype,"value",1),v([_({attribute:!1})],$n.prototype,"constraint",1),v([_({attribute:!1})],$n.prototype,"mode",1)});var Vn,fl=f(()=>{"use strict";Vn=class{constructor(e){this.host=e;this.abortController=null;e.addController(this)}get active(){return this.abortController!==null}hostDisconnected(){this.cancel()}start(e){this.cancel();let n=new AbortController;this.abortController=n;let r={signal:n.signal};document.addEventListener("pointermove",e.move,r),document.addEventListener("pointerup",o=>{this.cancel(),e.end(o)},r),document.addEventListener("pointercancel",()=>{this.cancel(),e.cancel()},r)}cancel(){this.abortController?.abort(),this.abortController=null}}});function _s(t,e){return(!e.measurementSystem||!t.system||t.system===e.measurementSystem)&&(!e.measurementScope||!t.scope||t.scope===e.measurementScope)&&(!e.measurementMeasure||!t.measure||t.measure===e.measurementMeasure)&&(!e.basis||!t.basis||t.basis===e.basis)}function dE(t,e,n){return t.find(r=>r.key===n.unitKey)??t.find(r=>!!r.system&&r.system===n.measurementSystem&&_s(r,n))??t.find(r=>_s(r,n))??t.find(r=>r.key===e?.defaultUnitKey&&_s(r,n))??t[0]??null}function fE(t,e,n){return typeof e?.fromCanonical=="function"?Number(e.fromCanonical(t,n)??0):t}function pE(t,e,n){return typeof e?.toCanonical=="function"?Number(e.toCanonical(t,n)??0):t}var hE=f(()=>{"use strict"});function Po(t){t.item=null,t.value=0,t.constraint={},t.role="composition",t.hits=[],t.rangeMin=0,t.rangeMax=100,t.precision=null,t.defaultValue=null,t.thresholds=[],t.unitOptions=[],t.unitKey="",t.measurementSystem="",t.basis="",t.measurementScope="",t.measurementMeasure="",t.context=null,t.disabled=!1,t.theme={},t.palette=_e,t.variant="default",t.unitDisplay="picker",t.activeEmphasis=!0}var Oo,Fn,pl=f(()=>{"use strict";hE();qt();hn();xe();Oo={item:{attribute:!1},value:{attribute:!1},constraint:{attribute:!1},role:{attribute:!1},hits:{attribute:!1},rangeMin:{attribute:!1},rangeMax:{attribute:!1},precision:{attribute:!1},defaultValue:{attribute:!1},thresholds:{attribute:!1},unitOptions:{attribute:!1},unitKey:{attribute:!1},measurementSystem:{attribute:!1},basis:{attribute:!1},measurementScope:{attribute:!1},measurementMeasure:{attribute:!1},context:{attribute:!1},disabled:{attribute:!1},palette:{attribute:!1},variant:{attribute:!1},unitDisplay:{attribute:!1},activeEmphasis:{attribute:!1}};Fn=class{constructor(e){this.host=e;e.addController(this)}hostConnected(){}get state(){let e=this.host;return{item:e.item,value:e.value,constraint:e.constraint,role:e.role,hits:e.hits,rangeMin:e.rangeMin,rangeMax:e.rangeMax,precision:e.precision,defaultValue:e.defaultValue,thresholds:e.thresholds,unitOptions:e.unitOptions,unitKey:e.unitKey,measurementSystem:e.measurementSystem,basis:e.basis,measurementScope:e.measurementScope,measurementMeasure:e.measurementMeasure,context:e.context,disabled:e.disabled,theme:e.theme,palette:e.palette,variant:e.variant,unitDisplay:e.unitDisplay,activeEmphasis:e.activeEmphasis}}setState(e){let n=this.host;Object.assign(n,{item:e.item??null,value:Number(e.value??0),constraint:e.constraint??{},role:e.role??"composition",hits:e.hits??[],rangeMin:Number(e.rangeMin??0),rangeMax:Number(e.rangeMax??100),precision:e.precision==null?null:Number(e.precision),defaultValue:e.defaultValue==null?null:Number(e.defaultValue),thresholds:e.thresholds??e.item?.thresholds??[],unitOptions:e.unitOptions??e.item?.unitOptions??[],unitKey:e.unitKey??"",measurementSystem:e.measurementSystem??"",basis:e.basis??"",measurementScope:e.measurementScope??"",measurementMeasure:e.measurementMeasure??"",context:e.context??null,disabled:!!e.disabled,theme:e.theme??{},palette:ys(e.palette),variant:e.variant??"default",unitDisplay:e.unitDisplay??"picker",activeEmphasis:e.activeEmphasis!==!1})}patchState(e){Object.assign(this.host,e)}viewState(){let e=this.host,n=this.domain(),r=this.min(),o=this.max(),i=K(e.value,n.min,n.max),s=this.places(),a=this.toDisplay(r),c=this.toDisplay(o),l=this.toDisplay(i);return{domain:n,min:r,max:o,value:i,displayMin:a,displayMax:c,displayValue:l,locked:!!e.constraint.locked,places:s,unit:this.unit(),itemColor:dt(e.item,0,e.palette),minPct:this.pct(r),maxPct:this.pct(o),valuePct:this.pct(i),minText:Be(a,s),maxText:Be(c,s),valueText:Be(l,s),domainMinText:Be(this.toDisplay(n.min),s),domainMaxText:Be(this.toDisplay(n.max),s),displayStep:this.displayStep(),thresholdKey:this.thresholdFor(i)?.key??""}}changeUnit(e){let n=this.host.unitOptions.find(r=>r.key===e);n&&(this.host.unitKey=e,O(this.host,"vector-change",{source:"inspector",role:"measurement",transactionId:P("rail-unit"),key:this.host.item?.key,unitKey:e,basis:n.basis,measurementScope:n.scope??this.host.measurementScope,measurementMeasure:n.measure??this.host.measurementMeasure,measurementSystem:n.system??this.host.measurementSystem}))}handleValue(e){return e==="min"?this.min():e==="max"?this.max():this.host.value}domain(){let e=Number.isFinite(Number(this.host.item?.min))?Number(this.host.item?.min):this.host.rangeMin,n=Number.isFinite(Number(this.host.item?.max))?Number(this.host.item?.max):this.host.rangeMax;return{min:Number.isFinite(e)?e:0,max:Number.isFinite(n)?n:100}}min(){return Number.isFinite(Number(this.host.constraint.min))?Number(this.host.constraint.min):this.domain().min}max(){return Number.isFinite(Number(this.host.constraint.max))?Number(this.host.constraint.max):this.domain().max}pct(e){let n=this.domain();return K((Number(e??0)-n.min)/(n.max-n.min||1)*100,0,100)}step(){let e=this.unitOption();if(e?.step!=null&&Number.isFinite(Number(e.step))){let n=this.toCanonical(0);return Math.abs(this.toCanonical(Number(e.step))-n)||Number(e.step)}return Number(this.host.constraint.step??this.host.item?.step??1)}places(){if(Number.isFinite(this.host.precision))return Math.max(0,Number(this.host.precision));let e=this.unitOption();return e?.precision!=null&&Number.isFinite(Number(e.precision))?Math.max(0,Number(e.precision)):Ao(this.displayStep())}unit(){return this.unitOption()?.symbol??(this.host.role==="composition"?"%":this.host.item?.unit??"")}unitOption(){return dE(this.host.unitOptions,this.host.item,this.preferences())}unitMatchesOption(e){return e?_s(e,this.preferences()):!1}toDisplay(e){return fE(Number(e??0),this.unitOption(),this.conversionContext())}toCanonical(e){return pE(Number(e??0),this.unitOption(),this.conversionContext())}displayStep(){let e=this.unitOption();if(e?.step!=null&&Number.isFinite(Number(e.step)))return Number(e.step);let n=this.toDisplay(0);return Math.abs(this.toDisplay(this.step())-n)||this.step()}thresholdFor(e){return this.host.thresholds.find(n=>Number(e)>=Number(n.min)&&Number(e)<=Number(n.max))??null}handleHitClass(e){let n=this.host.hits.filter(r=>r.key===this.host.item?.key);return e==="min"&&n.some(r=>r.type==="min")||e==="max"&&n.some(r=>r.type==="max")||e==="value"&&n.some(r=>["locked","unreachable","solver","sum"].includes(r.type))?"hit":""}preferences(){let e=this.host;return{unitKey:e.unitKey,measurementSystem:e.measurementSystem,basis:e.basis,measurementScope:e.measurementScope,measurementMeasure:e.measurementMeasure}}conversionContext(){let e=this.host,n=e.context&&typeof e.context=="object"?e.context:{};return T(m({},n),{item:e.item,measurementSystem:e.measurementSystem,basis:e.basis,measurementScope:e.measurementScope,measurementMeasure:e.measurementMeasure})}}});function hl(t){return ue(t.thresholds,e=>e.key,e=>{let n=t.percent(e.min),r=t.percent(e.max),o=t.activeThreshold(t.value)?.key===e.key;return C`
        <span
          class=${G({"threshold-band":!0,[e.severity??"ok"]:!0,active:o})}
          ${de({"--threshold-start":`${n}%`,"--threshold-width":`${Math.max(0,r-n)}%`,"--threshold-color":e.color??null})}
          title=${e.label??e.key}
        ></span>
      `})}var Np=f(()=>{"use strict";W();wt();Ot();pn()});var mE,gE=f(()=>{"use strict";W();mE=Z`:host {
  display: block;
}

* {
  box-sizing: border-box;
}

.empty,
.rail-control {
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  padding: 12px;
  background: var(--_vector-surface-alt);
}

.empty {
  color: var(--_vector-muted);
}

header {
  display: grid;
  grid-template-columns: 14px minmax(0, 1fr) auto auto;
  gap: 8px;
  align-items: center;
  margin-bottom: 18px;
}

.swatch {
  inline-size: 12px;
  block-size: 12px;
  border-radius: 999px;
  background: var(--item-color);
}

strong {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

output {
  color: var(--_vector-ink);
  font-weight: 700;
}

button,
input {
  font: inherit;
}

.lock-button {
  display: inline-grid;
  place-items: center;
  inline-size: 34px;
  min-height: 30px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
  padding: 0;
  cursor: pointer;
  font-weight: 800;
  line-height: 1;
}

.lock-button.locked {
  border-color: var(--_vector-accent-strong);
  background: var(--_vector-accent-soft);
  color: var(--_vector-accent-strong);
  font-weight: 700;
}

.rail-body {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  align-items: center;
  gap: 8px;
}

.embedded {
  padding: 10px 12px 12px;
}

.embedded .rail-body {
  grid-template-columns: minmax(0, 1fr) 34px;
}

.rail {
  position: relative;
  block-size: 86px;
  margin: 0 12px 4px;
  touch-action: none;
}

.embedded .rail {
  margin-inline: 12px 4px;
}

.full-track,
.allowed-track,
.threshold-track {
  position: absolute;
  left: 0;
  right: 0;
  top: 40px;
  block-size: 8px;
  border-radius: 999px;
}

.full-track {
  background: var(--_vector-track);
}

.threshold-track {
  overflow: hidden;
  background: transparent;
  z-index: 1;
}

.threshold-band {
  position: absolute;
  top: 0;
  bottom: 0;
  left: var(--threshold-start);
  width: var(--threshold-width);
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-accent-strong) 35%, transparent));
}

.threshold-band.ok {
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-ok) 42%, transparent));
}

.threshold-band.warn {
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-warning) 46%, transparent));
}

.threshold-band.danger {
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-danger) 48%, transparent));
}

.threshold-band.active {
  box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--_vector-ink) 30%, transparent);
}

.allowed-track {
  left: var(--range-start);
  right: auto;
  width: var(--range-width);
  background: color-mix(in srgb, var(--_vector-allowed) 38%, transparent);
  box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--_vector-accent-strong) 16%, transparent);
}

.default-marker {
  position: absolute;
  top: 32px;
  block-size: 24px;
  inline-size: 2px;
  background: var(--_vector-ink-soft);
  border-radius: 999px;
  opacity: 0.58;
  left: var(--rail-position);
}

.handle {
  position: absolute;
  top: 28px;
  display: grid;
  place-items: center;
  inline-size: 34px;
  block-size: 34px;
  margin-left: -17px;
  border: 0;
  background: transparent;
  color: var(--_vector-ink);
  cursor: ew-resize;
  padding: 0;
  outline-offset: 4px;
  left: var(--rail-position);
}

.handle,
.default-marker {
  z-index: 2;
}

.handle-dot {
  display: grid;
  place-items: center;
  inline-size: 18px;
  block-size: 18px;
  border: 2px solid var(--_vector-accent);
  background: var(--_vector-surface);
  color: var(--_vector-accent-strong);
  font-size: 0.65rem;
  font-weight: 800;
  line-height: 1;
}

.min .handle-dot,
.max .handle-dot {
  border-radius: 999px;
}

.value .handle-dot {
  inline-size: 22px;
  block-size: 22px;
  border-color: var(--_vector-accent-strong);
  background: var(--_vector-accent-strong);
  color: var(--_vector-surface);
  transform: rotate(45deg);
}

.value.locked .handle-dot {
  border-color: var(--_vector-accent-strong);
  background: var(--_vector-accent-soft);
  color: var(--_vector-accent-strong);
  transform: none;
  border-radius: 7px;
}

.handle-label,
.handle-value {
  position: absolute;
  opacity: 0;
  pointer-events: none;
  transition: opacity 120ms ease, transform 120ms ease;
  white-space: nowrap;
}

.handle-label {
  top: -20px;
  color: var(--_vector-muted);
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0;
}

.handle-value {
  top: 34px;
  color: var(--_vector-ink);
  font-size: 0.74rem;
  font-weight: 700;
}

.rail:hover .handle-label,
.rail:hover .handle-value,
.handle:focus .handle-label,
.handle:focus .handle-value,
.rail-control.dragging .handle-label,
.rail-control.dragging .handle-value,
.handle.active .handle-label,
.handle.active .handle-value {
  opacity: 1;
  transform: translateY(-1px);
}

.handle.hit .handle-dot {
  border-color: var(--_vector-danger);
  animation: railPulse 700ms ease-in-out infinite alternate;
}

.exact-row {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  margin-top: 6px;
}

.embedded .exact-row {
  margin-right: 42px;
}

label {
  display: grid;
  gap: 5px;
  color: var(--_vector-muted);
  font-size: 0.76rem;
}

input,
select {
  min-height: 32px;
  width: 100%;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
  padding: 0 8px;
}

input:disabled,
select:disabled,
button:disabled {
  opacity: 0.58;
  background: var(--_vector-surface-strong);
  cursor: default;
}

.unit-row {
  margin-top: 8px;
}

.disabled {
  opacity: 0.65;
  pointer-events: none;
}

@keyframes railPulse {
  from {
    transform: scale(1);
  }
  to {
    transform: scale(1.28);
  }
}
@media (max-width: 720px) {
  header {
    grid-template-columns: 14px minmax(0, 1fr) auto;
  }
  .lock-button {
    grid-column: 1/-1;
    justify-self: start;
  }
  .embedded .lock-button {
    grid-column: auto;
    justify-self: stretch;
  }
  .exact-row {
    grid-template-columns: 1fr;
  }
  .embedded .exact-row {
    margin-right: 0;
  }
}`});var vE,yE,bE,Lo,Rp=f(()=>{"use strict";W();nt();wt();pn();fl();pl();He();Np();gE();qt();hn();xe();Ue();Lo=class extends te{constructor(){super();N(this,vE,"composition");this.scalar=new Fn(this);this.pointerDrag=new Vn(this);N(this,yE);N(this,bE,null);Po(this)}static{this.properties=Oo}get role(){return M(this,vE)}set role(n){R(this,vE,n)}get railElement(){return M(this,yE)}set railElement(n){R(this,yE,n)}static{this.styles=[se,mE]}get _drag(){return M(this,bE)}set _drag(n){R(this,bE,n)}set state(n){this.scalar.setState(n)}get state(){return this.scalar.state}render(){if(!this.item)return C` <div class="empty">Select an item.</div> `;let n=this.scalar.domain(),r=this.scalar.min(),o=this.scalar.max(),i=K(this.value,n.min,n.max),s=!!this.constraint.locked,a=this.scalar.viewState(),c=this.scalar.pct(r),l=this.scalar.pct(o),u=this.scalar.pct(i),d=a.unit,h=dt(this.item,0,this.palette),p=a.minText,g=a.maxText,E=a.valueText,b=this.variant==="embedded",D=s?"Unlock value":"Lock value",V=()=>C`
      <button
        class=${G({"lock-button":!0,locked:s})}
        data-lock
        type="button"
        aria-pressed=${String(s)}
        aria-label=${D}
        title=${D}
        ?disabled=${this.disabled}
        @click=${()=>this._toggleLock()}
      >
        ${s?gs:O_}
      </button>
    `;return C`
      <section
        class=${G({"rail-control":!0,embedded:b,disabled:this.disabled,dragging:!!this._drag})}
        data-threshold=${a.thresholdKey||""}
      >
        ${b?"":C`<header>
              <span
                class="swatch"
                ${de({"--item-color":h})}
              ></span>
              <strong>${this.item.label||Q(this.item.key)}</strong>
              <output>${E}${d}</output>
              ${V()}
            </header>`}

        <div class="rail-body">
          <div class="rail" data-rail>
            <div class="full-track"></div>
            <div
              class="allowed-track"
              ${de({"--range-start":`${c}%`,"--range-width":`${Math.max(0,l-c)}%`})}
            ></div>
            <div class="threshold-track" aria-hidden="true">
              ${hl({thresholds:this.thresholds,value:this.value,percent:I=>this.scalar.pct(I),activeThreshold:I=>this.scalar.thresholdFor(I)})}
            </div>
            ${this.defaultValue==null?"":C`<span
                  class="default-marker"
                  ${de({"--rail-position":`${this.scalar.pct(this.defaultValue)}%`})}
                  title="Default"
                ></span>`}
            <button
              class=${G({handle:!0,min:!0,hit:!!this.scalar.handleHitClass("min"),active:this._drag?.handle==="min"})}
              data-handle="min"
              ${de({"--rail-position":`${c}%`})}
              type="button"
              aria-label=${`Minimum ${p}${d}`}
              ?disabled=${this.disabled}
              @pointerdown=${I=>this._startDrag(I,"min")}
              @keydown=${I=>this._keydown(I,"min")}
            >
              <span class="handle-label">min</span>
              <span class="handle-dot"></span>
              <span class="handle-value">${p}${d}</span>
            </button>
            <button
              class=${G({handle:!0,value:!0,locked:s,hit:!!this.scalar.handleHitClass("value"),active:this._drag?.handle==="value"})}
              data-handle="value"
              ${de({"--rail-position":`${u}%`})}
              type="button"
              aria-label=${`Value ${E}${d}`}
              ?disabled=${this.disabled}
              @pointerdown=${I=>this._startDrag(I,"value")}
              @keydown=${I=>this._keydown(I,"value")}
            >
              <span class="handle-label">value</span>
              <span class="handle-dot">${s?gs:""}</span>
              <span class="handle-value">${E}${d}</span>
            </button>
            <button
              class=${G({handle:!0,max:!0,hit:!!this.scalar.handleHitClass("max"),active:this._drag?.handle==="max"})}
              data-handle="max"
              ${de({"--rail-position":`${l}%`})}
              type="button"
              aria-label=${`Maximum ${g}${d}`}
              ?disabled=${this.disabled}
              @pointerdown=${I=>this._startDrag(I,"max")}
              @keydown=${I=>this._keydown(I,"max")}
            >
              <span class="handle-label">max</span>
              <span class="handle-dot"></span>
              <span class="handle-value">${g}${d}</span>
            </button>
          </div>
          ${b?V():""}
        </div>

        <div class="exact-row">
          <label
            >Min<input
              data-exact="min"
              type="number"
              min=${a.domainMinText}
              max=${a.domainMaxText}
              step=${a.displayStep}
              .value=${p}
              ?disabled=${this.disabled}
              @change=${I=>this._emitInputTarget("min",Number(I.currentTarget.value),"vector-change",P("rail-input"))}
          /></label>
          <label
            >Value<input
              data-exact="value"
              type="number"
              min=${a.domainMinText}
              max=${a.domainMaxText}
              step=${a.displayStep}
              .value=${E}
              ?disabled=${s||this.disabled}
              @change=${I=>this._emitInputTarget("value",Number(I.currentTarget.value),"vector-change",P("rail-input"))}
          /></label>
          <label
            >Max<input
              data-exact="max"
              type="number"
              min=${a.domainMinText}
              max=${a.domainMaxText}
              step=${a.displayStep}
              .value=${g}
              ?disabled=${this.disabled}
              @change=${I=>this._emitInputTarget("max",Number(I.currentTarget.value),"vector-change",P("rail-input"))}
          /></label>
        </div>
        ${this.unitOptions.length>1?C`
              <label class="unit-row"
                >Unit
                <select
                  data-unit
                  .value=${this.scalar.unitOption()?.key||""}
                  ?disabled=${this.disabled}
                  @change=${I=>this.scalar.changeUnit(I.currentTarget.value)}
                >
                  ${this.unitOptions.map(I=>C`<option
                        value=${I.key}
                        ?selected=${I.key===this.scalar.unitOption()?.key}
                      >
                        ${I.label}
                      </option>`)}
                </select>
              </label>
            `:ie}
      </section>
    `}_startDrag(n,r){if(!(this.disabled||n.button>0)){if(n.preventDefault(),r==="value"&&this.constraint.locked){this._emitLockedHit();return}this._drag={handle:r,transactionId:P("rail"),startX:n.clientX,startValue:this.scalar.handleValue(r),latestTarget:this.scalar.handleValue(r)},this.pointerDrag.start({move:o=>this._dragMove(o),end:()=>this._dragEnd(),cancel:()=>this._cancelDrag()})}}_dragMove(n){if(!this._drag)return;let r=this.railElement,o=Math.max(1,r?.getBoundingClientRect?.().width||1),i=this.scalar.domain(),s=(n.clientX-this._drag.startX)/o*(i.max-i.min),a=Ce(this._drag.startValue+s,this.scalar.step());this._drag.latestTarget=a,this._emitHandleTarget(this._drag.handle,a,"vector-preview",this._drag.transactionId)}_dragEnd(){if(!this._drag)return;let n=this._drag.handle,r=this._drag.transactionId,o=this._drag.latestTarget??this._drag.startValue;this._emitHandleTarget(n,o,"vector-change",r),this._drag=null,this.requestUpdate()}_cancelDrag(){this._drag&&(O(this,"vector-cancel",{source:"constraint",role:this.role,transactionId:this._drag.transactionId,key:this.item?.key}),this._drag=null,this.requestUpdate())}_keydown(n,r){if(this.disabled)return;let o=this.scalar.step(),i=null;if((n.key===" "||n.key==="Enter")&&r==="value"&&this.constraint.locked){n.preventDefault(),this._toggleLock();return}if(r==="value"&&this.constraint.locked){["ArrowLeft","ArrowDown","ArrowRight","ArrowUp","Home","End"].includes(n.key)&&(n.preventDefault(),this._emitLockedHit());return}if(n.key==="ArrowRight"||n.key==="ArrowUp"?i=this.scalar.handleValue(r)+o:n.key==="ArrowLeft"||n.key==="ArrowDown"?i=this.scalar.handleValue(r)-o:n.key==="Home"?i=this.scalar.domain().min:n.key==="End"?i=this.scalar.domain().max:n.key==="Escape"&&this._cancelDrag(),i==null)return;n.preventDefault();let s=P("rail-key");this._emitHandleTarget(r,i,"vector-preview",s),this._emitHandleTarget(r,i,"vector-change",s)}_emitHandleTarget(n,r,o,i){if(!this.item?.key)return;let s=this.scalar.unitOption();if(n==="value"){if(this.constraint.locked){this._emitLockedHit(i);return}let u=Ce(r,this.scalar.step()),d=K(u,this.scalar.min(),this.scalar.max()),h=this.scalar.thresholdFor(d),p=[];u<this.scalar.min()&&p.push(he(this.item.key,"min",`${this.item.label||Q(this.item.key)} hit minimum ${w(this.scalar.toDisplay(this.scalar.min()),this.scalar.places())}${this.scalar.unit()}.`)),u>this.scalar.max()&&p.push(he(this.item.key,"max",`${this.item.label||Q(this.item.key)} hit maximum ${w(this.scalar.toDisplay(this.scalar.max()),this.scalar.places())}${this.scalar.unit()}.`));let g={source:"inspector",role:this.role,transactionId:i,key:this.item.key,previousValue:{[this.item.key]:this.value},requestedValue:{[this.item.key]:u},proposedValue:{[this.item.key]:d},delta:w(d-this.value,4),unitKey:s?.key,basis:s?.basis??this.basis??this.item?.basis,measurementScope:s?.scope??this.measurementScope,measurementMeasure:s?.measure??this.measurementMeasure,displayValue:this.scalar.toDisplay(d),canonicalValue:d,thresholdKey:h?.key,measurementSystem:this.measurementSystem||s?.system,hits:p};O(this,o,g),p.length&&O(this,"vector-constraint-hit",g);return}let a=this.scalar.domain(),c=Ce(K(r,a.min,a.max),this.scalar.step()),l=n==="min"?{min:c}:{max:c};O(this,o,{source:"constraint",role:"constraint",transactionId:i,key:this.item.key,constraintRole:this.role,constraint:l,unitKey:s?.key,basis:s?.basis||this.basis||this.item?.basis,measurementScope:s?.scope||this.measurementScope,measurementMeasure:s?.measure||this.measurementMeasure,displayValue:this.scalar.toDisplay(c),canonicalValue:c,measurementSystem:this.measurementSystem||s?.system})}_emitInputTarget(n,r,o,i){this._emitHandleTarget(n,this.scalar.toCanonical(r),o,i)}_toggleLock(){this.disabled||O(this,"vector-change",{source:"constraint",role:"constraint",transactionId:P("rail-lock"),key:this.item?.key,constraintRole:this.role,constraint:{locked:!this.constraint.locked}})}_emitLockedHit(n=P("rail-locked")){let r=he(this.item?.key??"constraint","locked",`${this.item?.label??Q(this.item?.key??"constraint")} is locked.`);O(this,"vector-constraint-hit",{source:"constraint",role:this.role,transactionId:n,key:this.item?.key,unitKey:this.scalar.unitOption()?.key,basis:this.scalar.unitOption()?.basis||this.basis||this.item?.basis,measurementScope:this.scalar.unitOption()?.scope||this.measurementScope,measurementMeasure:this.scalar.unitOption()?.measure||this.measurementMeasure,measurementSystem:this.measurementSystem||this.scalar.unitOption()?.system,hits:[r]})}};vE=new WeakMap,yE=new WeakMap,bE=new WeakMap,v([fn("[data-rail]")],Lo.prototype,"railElement",1),v([No()],Lo.prototype,"_drag",1)});var ml,kp=f(()=>{"use strict";bs();ml=class extends ze{constructor(){super(),this._listType="ingredient",this._searchPlaceholder="Search additives"}}});var _E,EE=f(()=>{"use strict";W();_E=Z`:host {
  display: block;
}

.empty,
.inspector {
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  padding: 12px;
  background: var(--_vector-surface-alt);
}

.empty {
  color: var(--_vector-muted);
}

header {
  display: grid;
  grid-template-columns: 14px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  margin-bottom: 10px;
}

.swatch {
  inline-size: 12px;
  block-size: 12px;
  border-radius: 999px;
  background: var(--item-color);
}

.range {
  width: 100%;
}

.stepper {
  display: grid;
  grid-template-columns: 38px 1fr 38px;
  gap: 6px;
  margin-top: 8px;
}

button,
input {
  min-height: 34px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
}

input {
  padding: 0 8px;
}`});var gn,Ap=f(()=>{"use strict";W();nt();pn();EE();He();qt();hn();xe();Ue();gn=class extends te{static{this.styles=[se,_E]}#e;get item(){return this.#e}set item(e){this.#e=e}#t;get value(){return this.#t}set value(e){this.#t=e}#n;get constraint(){return this.#n}set constraint(e){this.#n=e}#r;get role(){return this.#r}set role(e){this.#r=e}#o;get palette(){return this.#o}set palette(e){this.#o=e}constructor(){super(),this.item=null,this.value=0,this.constraint={},this.role="composition",this.theme={},this.palette=_e}render(){if(!this.item)return C` <div class="empty">Select an oil or quality.</div> `;let e=Number(this.constraint.min??this.item.min??0),n=Number(this.constraint.max??this.item.max??100),r=Number(this.constraint.step??this.item.step??(this.role==="composition"?.1:1));return C`
      <section class="inspector">
        <header>
          <span
            class="swatch"
            ${de({"--item-color":dt(this.item,0,this.palette)})}
          ></span>
          <strong>${this.item.label||Q(this.item.key)}</strong>
          <span
            >${w(this.value,1)}${this.role==="composition"?"%":""}</span
          >
        </header>
        <input
          class="range"
          type="range"
          min=${e}
          max=${n}
          step=${r}
          .value=${String(this.value)}
          ?disabled=${!!this.constraint.locked}
          @input=${o=>this._emit(Number(o.currentTarget.value),"vector-preview")}
          @change=${o=>this._emit(Number(o.currentTarget.value),"vector-change")}
        />
        <div class="stepper">
          <button
            data-delta=${-r}
            ?disabled=${!!this.constraint.locked}
            @click=${()=>this._emit(this.value-r,"vector-change")}
          >
            -
          </button>
          <input
            class="exact"
            type="number"
            min=${e}
            max=${n}
            step=${r}
            .value=${String(w(this.value,2))}
            ?disabled=${!!this.constraint.locked}
            @change=${o=>this._emit(Number(o.currentTarget.value),"vector-change")}
          />
          <button
            data-delta=${r}
            ?disabled=${!!this.constraint.locked}
            @click=${()=>this._emit(this.value+r,"vector-change")}
          >
            +
          </button>
        </div>
      </section>
    `}_emit(e,n){let r=this.item;if(!r)return;let o=K(e,this.constraint.min??r.min??0,this.constraint.max??r.max??100);O(this,n,{source:"inspector",role:this.role,transactionId:P("inspector"),key:r.key,requestedValue:{[r.key]:o},proposedValue:{[r.key]:o},delta:w(o-this.value,4)})}};v([_({attribute:!1})],gn.prototype,"item",1),v([_({attribute:!1})],gn.prototype,"value",1),v([_({attribute:!1})],gn.prototype,"constraint",1),v([_({attribute:!1})],gn.prototype,"role",1),v([_({attribute:!1})],gn.prototype,"palette",1)});var wE,IE=f(()=>{"use strict";W();wE=Z`:host {
  display: block;
}

.chart-shell {
  display: grid;
  gap: 12px;
  align-items: center;
  justify-items: center;
  min-width: 0;
}

.pie-frame {
  position: relative;
  display: grid;
  place-items: center;
  inline-size: 100%;
  max-inline-size: 330px;
  min-inline-size: 220px;
}

.pie {
  inline-size: 100%;
  touch-action: none;
}

.decorator-layer {
  position: absolute;
  inset: 0;
  pointer-events: none;
}

::slotted([slot=slice-decorator]) {
  position: absolute;
  left: var(--decorator-x, -999px);
  top: var(--decorator-y, -999px);
  transform: translate(-50%, -50%) translate(var(--decorator-offset-x, 0px), var(--decorator-offset-y, 0px));
  pointer-events: none;
}

.base {
  fill: var(--_vector-surface-strong);
}

.hole {
  fill: var(--_vector-surface);
  stroke: var(--_vector-line);
  stroke-width: 1;
}

.slice {
  stroke: var(--_vector-surface);
  stroke-width: 1.5;
  cursor: ns-resize;
  transition: transform 120ms ease, stroke-width 120ms ease, filter 120ms ease;
  outline: none;
}

.slice:hover,
.slice:focus,
.slice.selected {
  filter: brightness(1.04);
  stroke: var(--_vector-ink);
  stroke-width: 2;
}

.slice.hit {
  stroke: var(--_vector-danger);
  stroke-width: 3;
  animation: pulse 900ms ease-in-out infinite alternate;
}

.center {
  font: 700 18px system-ui, sans-serif;
  fill: var(--_vector-ink);
}

.center-label {
  font: 11px system-ui, sans-serif;
  fill: var(--_vector-muted);
}

.legend {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(138px, 1fr));
  gap: 6px;
  inline-size: 100%;
  min-width: 0;
}

.legend-item {
  display: grid;
  grid-template-columns: 14px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  min-height: 32px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
  text-align: left;
  cursor: pointer;
}

.legend-item.selected {
  border-color: var(--_vector-accent-strong);
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--_vector-accent-strong) 12%, transparent);
}

.after-legend {
  inline-size: 100%;
  min-width: 0;
}

::slotted([slot=after-legend]) {
  display: block;
  inline-size: 100%;
}

.swatch {
  inline-size: 12px;
  block-size: 12px;
  border-radius: 999px;
  background: var(--item-color);
}

.disabled {
  opacity: 0.55;
  pointer-events: none;
}

@keyframes pulse {
  from {
    stroke-width: 2.5;
  }
  to {
    stroke-width: 5;
  }
}`});function vl(t,e,n,r,o){let i=Number(t.min??0),s=Number(t.max??100),a=s===i?0:K((Number(e||0)-i)/(s-i),0,1);return Es($o(n,r),o*a)}function $o(t,e){return-Math.PI/2+t*(Math.PI*2/Math.max(1,e))}function Es(t,e){return{x:w(Math.cos(t)*e,2),y:w(Math.sin(t)*e,2)}}function CE(t){let e=Math.max(1,Math.hypot(t.x,t.y));return{x:w(t.x+t.x/e*15,2),y:w(t.y+t.y/e*15+3,2)}}function xE(t,e){return Vo(Array.from({length:t},(n,r)=>Es($o(r,t),e)))}function DE(t,e){return SE(t)-SE(e)}function SE(t){let e=t.dataset||{};return"lockBadge"in e?0:e.limitType==="min"?1:e.limitType==="max"?2:3}function Vo(t){return t.filter(e=>e!==null).map(e=>`${e.x},${e.y}`).join(" ")}function TE(t,e,n){if(Math.abs(e-t)>=359.99)return["M 0 0",`m ${-n}, 0`,`a ${n},${n} 0 1,0 ${n*2},0`,`a ${n},${n} 0 1,0 ${-n*2},0`,"Z"].join(" ");let r=gl(t,n),o=gl(e,n),i=Math.abs(e-t)>180?1:0;return`M 0 0 L ${r.x} ${r.y} A ${n} ${n} 0 ${i} 1 ${o.x} ${o.y} Z`}function gl(t,e){let n=yl(t);return{x:w(Math.cos(n)*e,2),y:w(Math.sin(n)*e,2)}}function yl(t){return t*Math.PI/180}var Op=f(()=>{"use strict";xe()});var ME,NE,RE,kE,AE,OE,PE,LE,$E,VE,FE,jE,UE,HE,BE,we,Pp=f(()=>{"use strict";W();nt();wt();Ot();pn();fl();IE();He();qt();hn();Op();xe();Ue();we=class extends te{constructor(){super();this.pointerDrag=new Vn(this);N(this,ME);N(this,NE);N(this,RE);N(this,kE);N(this,AE);N(this,OE);N(this,PE);N(this,LE);N(this,$E);N(this,VE);N(this,FE);N(this,jE);N(this,UE);N(this,HE);N(this,BE);this.items=[],this.value={},this.constraints={},this.total=100,this.selectedKey="",this.hits=[],this.disabled=!1,this._drag=null,this.precision=null,this.centerValue=null,this.centerLabel="",this.centerPrecision=null,this.theme={},this.palette=_e,this._decoratorPositions=new Map}get decoratorSlot(){return M(this,ME)}set decoratorSlot(n){R(this,ME,n)}get chartShell(){return M(this,NE)}set chartShell(n){R(this,NE,n)}static{this.styles=[se,wE]}get _drag(){return M(this,RE)}set _drag(n){R(this,RE,n)}get items(){return M(this,kE)}set items(n){R(this,kE,n)}get value(){return M(this,AE)}set value(n){R(this,AE,n)}get constraints(){return M(this,OE)}set constraints(n){R(this,OE,n)}get total(){return M(this,PE)}set total(n){R(this,PE,n)}get selectedKey(){return M(this,LE)}set selectedKey(n){R(this,LE,n)}get hits(){return M(this,$E)}set hits(n){R(this,$E,n)}get disabled(){return M(this,VE)}set disabled(n){R(this,VE,n)}get precision(){return M(this,FE)}set precision(n){R(this,FE,n)}get centerValue(){return M(this,jE)}set centerValue(n){R(this,jE,n)}get centerLabel(){return M(this,UE)}set centerLabel(n){R(this,UE,n)}get centerPrecision(){return M(this,HE)}set centerPrecision(n){R(this,HE,n)}get palette(){return M(this,BE)}set palette(n){R(this,BE,n)}set state(n){this._setPieState(n??{})}get state(){return{items:this.items,value:this.value,constraints:this.constraints,total:this.total,selectedKey:this.selectedKey,hits:this.hits,disabled:this.disabled,precision:this.precision,centerValue:this.centerValue,centerLabel:this.centerLabel,centerPrecision:this.centerPrecision,theme:this.theme,palette:this.palette}}render(){let n=this._orderedEntries(),r=n.reduce((E,[,b])=>E+Number(b??0),0)||this.total,o=new Set(this.hits.map(E=>E.key)),i=this._places(),s=this.centerValue!=null,a=s?this.centerValue:r,c=this.centerPrecision==null?i:Number(this.centerPrecision||0),l=s?typeof a=="number"?Be(a,c):String(a):`${Be(r,i)}%`,u=this.centerLabel||"total",d=-90,h=new Map,p=ue(n,([E])=>E,([E,b],D)=>{let V=this._itemFor(E,D),I=dt(V,D,this.palette),le=!!(this.constraints[E]??{}).locked,ae=Number(b??0)/r*360,j=d,Ze=d+ae;d=Ze;let kr=E===this.selectedKey,Lt=o.has(E),$t=(j+Ze)/2,Gt=kr||Lt?5:0,S=Math.cos(yl($t))*Gt,Yp=Math.sin(yl($t))*Gt,Jp=le?" locked":"",Xp=Be(b,i),eh=gl($t,61.5);return h.set(E,{x:w((eh.x+S+104)/208*100,4),y:w((eh.y+Yp+104)/208*100,4)}),At`
        <g class="slice-wrap" transform="translate(${w(S,2)} ${w(Yp,2)})">
          <path
            class=${G({slice:!0,selected:kr,hit:Lt})}
            d="${TE(j,Ze,82)}"
            fill=${I}
            tabindex="0"
            role="button"
            aria-label=${`${V.label} ${Xp} percent${Jp}`}
            data-key=${E}
            @click=${()=>this._select(E)}
            @keydown=${jo=>this._keydown(jo,E)}
            @wheel=${jo=>this._wheel(jo,E)}
            @pointerdown=${jo=>this._startDrag(jo,E)}
          >
            <title>${V.label}: ${Xp}%${Jp}</title>
          </path>
        </g>
      `});this._decoratorPositions=h;let g=ue(n,([E])=>E,([E,b],D)=>{let V=this._itemFor(E,D),I=!!(this.constraints[E]||{}).locked;return C`
          <button
            class=${G({"legend-item":!0,selected:E===this.selectedKey})}
            data-key=${E}
            @click=${()=>this._select(E)}
          >
            <span
              class="swatch"
              ${de({"--item-color":dt(V,D,this.palette)})}
            ></span>
            <span>${V.label}</span>
            <strong
              >${I?`${gs} `:""}${Be(b,i)}%</strong
            >
          </button>
        `});return C`
      <div class=${G({"chart-shell":!0,disabled:this.disabled})}>
        <div class="pie-frame">
          <svg
            class="pie"
            viewBox="-104 -104 208 208"
            aria-label="Oil percentage pie chart"
          >
            <circle class="base" cx="0" cy="0" r="82"></circle>
            ${p}
            <circle class="hole" cx="0" cy="0" r="32"></circle>
            <text class="center" x="0" y="-3" text-anchor="middle">
              ${l}
            </text>
            <text class="center-label" x="0" y="13" text-anchor="middle">
              ${u}
            </text>
          </svg>
          <div class="decorator-layer">
            <slot
              name="slice-decorator"
              @slotchange=${()=>this._positionDecorators()}
            ></slot>
          </div>
        </div>
        <div class="legend">${g}</div>
        <div class="after-legend">
          <slot name="after-legend"></slot>
        </div>
      </div>
    `}updated(){this._positionDecorators()}_patchPieState(n){Object.assign(this,n)}_setPieState(n){let r={items:n.items??[],value:pe(n.value??{}),constraints:n.constraints??{},total:Number(n.total??100),selectedKey:n.selectedKey??"",hits:n.hits??[],disabled:!!n.disabled,precision:n.precision==null?null:Number(n.precision),centerValue:n.centerValue??null,centerLabel:n.centerLabel??"",centerPrecision:n.centerPrecision==null?null:Number(n.centerPrecision),theme:n.theme??{},palette:ys(n.palette)};this.items=r.items,this.value=r.value,this.constraints=r.constraints,this.total=r.total,this.selectedKey=r.selectedKey,this.hits=r.hits,this.disabled=r.disabled,this.precision=r.precision,this.centerValue=r.centerValue,this.centerLabel=r.centerLabel,this.centerPrecision=r.centerPrecision,this.theme=r.theme,this.palette=r.palette}positionDecorators(){this._positionDecorators()}_positionDecorators(){let n=this.decoratorSlot?.assignedElements({flatten:!0})??[],r=new Map;n.forEach(o=>{let i=o.dataset.key,s=i?this._decoratorPositions.get(i):void 0;if(!i||!s){o.hidden=!0;return}r.has(i)||r.set(i,{point:s,decorators:[]}),r.get(i)?.decorators.push(o)}),r.forEach(({point:o,decorators:i})=>{i.sort(DE),i.forEach((s,a)=>{let c=(a-(i.length-1)/2)*18;s.hidden=!1,s.style?.setProperty?.("--decorator-x",`${o.x}%`),s.style?.setProperty?.("--decorator-y",`${o.y}%`),s.style?.setProperty?.("--decorator-offset-x",`${w(c,2)}px`),s.style?.setProperty?.("--decorator-offset-y","0px")})})}_select(n){this.disabled||(this.selectedKey=n,O(this,"vector-select",{source:"pie",role:"composition",transactionId:P("select"),key:n}))}_startDrag(n,r){this.disabled||n.button>0||(n.preventDefault(),this._select(r),this._drag={key:r,transactionId:P("pie"),startX:n.clientX,startY:n.clientY,startValue:pe(this.value),latest:null},this.pointerDrag.start({move:o=>this._dragMove(o),end:o=>this._dragEnd(o),cancel:()=>this._cancelDrag()}))}_dragMove(n){if(!this._drag)return;let r=this.constraints[this._drag.key]??{},o=Number(r.step??1),i=this._drag.startY-n.clientY+(n.clientX-this._drag.startX),s=Ce(i/10,o),a=Number(this._drag.startValue[this._drag.key]??0)+s;this._previewTarget(a)}_dragEnd(n){if(!this._drag)return;let r=this.chartShell?.getBoundingClientRect();if(r?n.clientX<r.left-20||n.clientX>r.right+20||n.clientY<r.top-20||n.clientY>r.bottom+20:!1){this._cancelDrag();return}let i=this._drag.latest;i&&O(this,"vector-change",i),this._drag=null}_cancelDrag(){this._drag&&(O(this,"vector-cancel",{source:"pie",role:"composition",transactionId:this._drag.transactionId,previousValue:this._drag.startValue}),this._drag=null)}_wheel(n,r){if(this.disabled)return;n.preventDefault();let o=this.constraints[r]??{},i=Number(o.step??1),s=n.deltaY<0?1:-1,a=Number(this.value[r]??0)+s*i,c=P("pie-wheel"),l=this._detailForTarget(r,a,c,this.value);O(this,"vector-preview",l),O(this,"vector-change",l)}_keydown(n,r){let o=this.constraints[r]??{},i=Number(o.step??1),s=null;if(n.key==="ArrowUp"||n.key==="ArrowRight"?s=Number(this.value[r]??0)+i:n.key==="ArrowDown"||n.key==="ArrowLeft"?s=Number(this.value[r]??0)-i:n.key==="Home"?s=Number(o.min??0):n.key==="End"?s=Number(o.max??this.total):n.key==="Escape"&&(this._cancelDrag(),O(this,"vector-cancel",{source:"pie",role:"composition",transactionId:P("pie-cancel"),previousValue:this.value})),s==null)return;n.preventDefault();let a=this._detailForTarget(r,s,P("pie-key"),this.value);O(this,"vector-preview",a),O(this,"vector-change",a)}_previewTarget(n){let r=this._drag;if(!r)return;let o=this._detailForTarget(r.key,n,r.transactionId,r.startValue);r.latest=o,O(this,"vector-preview",o),o.hits?.length&&O(this,"vector-constraint-hit",o)}_detailForTarget(n,r,o,i){let s=Mr({value:i,key:n,target:r,constraints:this.constraints,items:this.items,total:this.total});return{source:"pie",role:"composition",transactionId:o,key:n,previousValue:i,requestedValue:T(m({},i),{[n]:r}),proposedValue:s.vector,delta:s.delta,hits:s.hits}}_orderedEntries(){let n=this.items.map(o=>o.key);return[...new Set([...n,...Object.keys(this.value||{})])].filter(o=>o in this.value).sort((o,i)=>{let s=this.items.find(c=>c.key===o),a=this.items.find(c=>c.key===i);return Number(s?.order||0)-Number(a?.order||0)}).map(o=>[o,Number(this.value[o]??0)])}_itemFor(n,r){return this.items.find(o=>o.key===n)??{key:n,label:Q(n),color:Sp(r,this.palette)}}_places(){if(Number.isFinite(this.precision))return Math.max(0,Number(this.precision));let n=Object.values(this.constraints||{}).map(r=>Number(r?.step)).filter(r=>Number.isFinite(r)&&r>0);return n.length?Math.max(...n.map(r=>Ao(r))):1}};ME=new WeakMap,NE=new WeakMap,RE=new WeakMap,kE=new WeakMap,AE=new WeakMap,OE=new WeakMap,PE=new WeakMap,LE=new WeakMap,$E=new WeakMap,VE=new WeakMap,FE=new WeakMap,jE=new WeakMap,UE=new WeakMap,HE=new WeakMap,BE=new WeakMap,v([fn('slot[name="slice-decorator"]')],we.prototype,"decoratorSlot",1),v([fn(".chart-shell")],we.prototype,"chartShell",1),v([No()],we.prototype,"_drag",1),v([_({attribute:!1})],we.prototype,"items",1),v([_({attribute:!1})],we.prototype,"value",1),v([_({attribute:!1})],we.prototype,"constraints",1),v([_({attribute:!1})],we.prototype,"total",1),v([_({attribute:!1})],we.prototype,"selectedKey",1),v([_({attribute:!1})],we.prototype,"hits",1),v([_({attribute:!1})],we.prototype,"disabled",1),v([_({attribute:!1})],we.prototype,"precision",1),v([_({attribute:!1})],we.prototype,"centerValue",1),v([_({attribute:!1})],we.prototype,"centerLabel",1),v([_({attribute:!1})],we.prototype,"centerPrecision",1),v([_({attribute:!1})],we.prototype,"palette",1)});var zE,qE=f(()=>{"use strict";W();zE=Z`:host {
  display: block;
}

* {
  box-sizing: border-box;
}

.empty,
.quantity-input {
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  padding: 10px 12px;
  background: var(--_vector-surface-alt);
}

.empty {
  color: var(--_vector-muted);
}

label {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(118px, 0.8fr);
  gap: 10px;
  align-items: center;
  color: var(--_vector-ink);
  font-size: 0.84rem;
  font-weight: 700;
}

.input-wrap {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  overflow: hidden;
}

input {
  min-width: 0;
  min-height: 34px;
  border: 0;
  background: transparent;
  color: var(--_vector-ink);
  padding: 0 8px;
  text-align: right;
  font: inherit;
}

.unit-suffix {
  min-width: 46px;
  padding: 0 8px 0 2px;
  color: var(--_vector-muted);
  font-size: 0.78rem;
  font-weight: 700;
}

input:disabled {
  opacity: 0.58;
  cursor: default;
}

.disabled {
  opacity: 0.65;
  pointer-events: none;
}

@media (max-width: 720px) {
  label {
    grid-template-columns: 1fr;
  }
}`});var WE,bl,Lp=f(()=>{"use strict";W();wt();qE();xe();Ue();pl();He();bl=class extends te{constructor(){super();N(this,WE,"composition");this.scalar=new Fn(this);Po(this)}static{this.properties=Oo}get role(){return M(this,WE)}set role(n){R(this,WE,n)}static{this.styles=[se,zE]}set state(n){this.scalar.setState(n)}get state(){return this.scalar.state}render(){if(!this.item)return C` <div class="empty">Select an item.</div> `;let n=this.scalar.viewState(),r=this.scalar.unitOption();return C`
      <section
        class=${G({"quantity-input":!0,disabled:this.disabled})}
      >
        <label>
          <span>${this.item.label||Q(this.item.key)}</span>
          <span class="input-wrap">
            <input
              data-quantity
              type="number"
              min=${n.domainMinText}
              max=${n.domainMaxText}
              step=${n.displayStep}
              .value=${n.valueText}
              ?disabled=${this.disabled}
              @change=${o=>this._emitQuantityValue(Number(o.currentTarget.value),"vector-change",P("quantity-input"))}
            />
            <span class="unit-suffix">${r?.symbol||n.unit}</span>
          </span>
        </label>
      </section>
    `}_emitQuantityValue(n,r,o){if(!this.item?.key)return;let i=this.scalar.unitOption(),s=this.scalar.toCanonical(n),a=Ce(s,this.scalar.step()),c=K(a,this.scalar.min(),this.scalar.max());O(this,r,{source:"inspector",role:this.role,transactionId:o,key:this.item.key,previousValue:{[this.item.key]:this.value},requestedValue:{[this.item.key]:a},proposedValue:{[this.item.key]:c},delta:w(c-this.value,4),unitKey:i?.key,basis:i?.basis||this.basis||this.item?.basis,measurementScope:i?.scope||this.measurementScope,measurementMeasure:i?.measure||this.measurementMeasure,displayValue:this.scalar.toDisplay(c),canonicalValue:c,measurementSystem:this.measurementSystem||i?.system,hits:[]})}};WE=new WeakMap});var GE,KE=f(()=>{"use strict";W();GE=Z`:host {
  display: block;
}

.chart-shell {
  display: grid;
  gap: 12px;
  place-items: center;
  min-width: 0;
}

.radar {
  inline-size: 100%;
  max-inline-size: 460px;
  min-inline-size: 260px;
  overflow: visible;
  touch-action: none;
}

.grid-ring {
  fill: none;
  stroke: var(--_vector-line);
  stroke-width: 0.8;
}

.axis {
  stroke: var(--_vector-line);
  stroke-width: 0.8;
}

.axis-label {
  font: 10px system-ui, sans-serif;
  fill: var(--_vector-ink-soft);
}

.axis-label.selected {
  font-weight: 800;
  fill: var(--_vector-accent-strong);
}

.preferred {
  fill: color-mix(in srgb, var(--_vector-accent-strong) 8%, transparent);
  stroke: color-mix(in srgb, var(--_vector-accent-strong) 35%, transparent);
  stroke-dasharray: 3 3;
}

.reachable {
  fill: color-mix(in srgb, var(--_vector-reach) 13%, transparent);
  stroke: color-mix(in srgb, var(--_vector-reach) 30%, transparent);
}

.current {
  fill: color-mix(in srgb, var(--_vector-accent) 22%, transparent);
  stroke: var(--_vector-accent);
  stroke-width: 2;
}

.preview {
  fill: color-mix(in srgb, var(--_vector-preview) 16%, transparent);
  stroke: var(--_vector-preview);
  stroke-width: 2;
  stroke-dasharray: 4 3;
}

.target-dot {
  fill: var(--_vector-preview);
  stroke: var(--_vector-surface);
  stroke-width: 1;
}

.marble {
  fill: var(--_vector-surface);
  stroke: var(--_vector-accent);
  stroke-width: 2;
  cursor: grab;
  outline: none;
}

.marble:hover,
.marble:focus,
.marble.selected {
  stroke: var(--_vector-ink);
  stroke-width: 3;
}

.marble.hit {
  stroke: var(--_vector-danger);
  animation: pulse 900ms ease-in-out infinite alternate;
}

.value-label {
  font: 700 10px system-ui, sans-serif;
  fill: var(--_vector-ink);
  stroke: var(--_vector-surface);
  stroke-width: 3;
  paint-order: stroke;
  pointer-events: none;
}

.value-label.selected {
  fill: var(--_vector-accent-strong);
}

.after-chart {
  inline-size: 100%;
  min-width: 0;
}

::slotted([slot=after-chart]) {
  display: block;
  inline-size: 100%;
}

.disabled {
  opacity: 0.55;
  pointer-events: none;
}

@keyframes pulse {
  from {
    r: 6;
  }
  to {
    r: 8;
  }
}`});var QE,ZE,YE,JE,XE,ew,tw,nw,rw,ow,iw,sw,aw,Ae,$p=f(()=>{"use strict";W();nt();wt();Ot();fl();KE();He();hn();Op();xe();Ue();Ae=class extends te{constructor(){super();this.pointerDrag=new Vn(this);N(this,QE);N(this,ZE,null);N(this,YE);N(this,JE);N(this,XE);N(this,ew);N(this,tw);N(this,nw);N(this,rw);N(this,ow);N(this,iw);N(this,sw);N(this,aw);this.axes=[],this.value={},this.previewValue=null,this.targetValue=null,this.reachableRange={},this.preferredRange={},this.selectedKey="",this.hits=[],this.disabled=!1,this._drag=null,this.precision=null,this.showValues=!1,this.theme={}}get svgElement(){return M(this,QE)}set svgElement(n){R(this,QE,n)}static{this.styles=[se,GE]}get _drag(){return M(this,ZE)}set _drag(n){R(this,ZE,n)}get axes(){return M(this,YE)}set axes(n){R(this,YE,n)}get value(){return M(this,JE)}set value(n){R(this,JE,n)}get previewValue(){return M(this,XE)}set previewValue(n){R(this,XE,n)}get targetValue(){return M(this,ew)}set targetValue(n){R(this,ew,n)}get reachableRange(){return M(this,tw)}set reachableRange(n){R(this,tw,n)}get preferredRange(){return M(this,nw)}set preferredRange(n){R(this,nw,n)}get selectedKey(){return M(this,rw)}set selectedKey(n){R(this,rw,n)}get hits(){return M(this,ow)}set hits(n){R(this,ow,n)}get disabled(){return M(this,iw)}set disabled(n){R(this,iw,n)}get precision(){return M(this,sw)}set precision(n){R(this,sw,n)}get showValues(){return M(this,aw)}set showValues(n){R(this,aw,n)}set state(n){this._setRadarState(n??{})}get state(){return{axes:this.axes,value:this.value,previewValue:this.previewValue,targetValue:this.targetValue,reachableRange:this.reachableRange,preferredRange:this.preferredRange,selectedKey:this.selectedKey,hits:this.hits,disabled:this.disabled,precision:this.precision,showValues:this.showValues,theme:this.theme}}render(){let n=this.axes,r=new Set(this.hits.map(b=>b.key)),o=this._pointsFor(this.value,90),i=this.previewValue?this._pointsFor(this.previewValue,90):[],s=this.targetValue?this._pointsFor(this.targetValue,90):[],a=this.previewValue??this.value,c=this._pointsFor(a,90),l=this._pointsForRange("preferredMax",90),u=this._pointsForRange("reachableMax",90),d=ue([.25,.5,.75,1],b=>b,b=>At`
      <polygon class="grid-ring" points=${xE(n.length,90*b)}></polygon>
    `),h=ue(n,b=>b.key,(b,D)=>{let V=Es($o(D,n.length),96),I=Es($o(D,n.length),112);return At`
        <line class="axis" x1="0" y1="0" x2="${V.x}" y2="${V.y}"></line>
        <text class=${G({"axis-label":!0,selected:b.key===this.selectedKey})}
          x=${I.x} y=${I.y} text-anchor="middle">${b.label}</text>
      `}),p=ue(n,b=>b.key,(b,D)=>{let V=vl(b,Number(a[b.key]??0),D,n.length,90),I=r.has(b.key),le=Be(a[b.key]??0,this._places(b));return At`
        <circle class=${G({marble:!0,selected:b.key===this.selectedKey,hit:I})}
          cx=${V.x} cy=${V.y} r="6" tabindex="0" role="slider"
          aria-label=${`${b.label} ${le}`} data-key=${b.key}
          @click=${()=>this._select(b.key)}
          @pointerdown=${ae=>this._startDrag(ae,b.key)}
          @keydown=${ae=>this._keydown(ae,b.key)}>
          <title>${b.label}: ${le}</title>
        </circle>
      `}),g=this.showValues?ue(c,(b,D)=>n[D]?.key||D,(b,D)=>{let V=n[D],I=CE(b);return At`
        <text class=${G({"value-label":!0,selected:V.key===this.selectedKey})}
          x=${I.x} y=${I.y} text-anchor="middle">
          ${Be(a[V.key]??0,this._places(V))}
        </text>
      `}):"",E=ue(s,(b,D)=>n[D]?.key||D,(b,D)=>{let V=n[D];return At`<circle class="target-dot" cx=${b.x} cy=${b.y} r="3.5"><title>${V.label} target</title></circle>`});return C`
      <div class=${G({"chart-shell":!0,disabled:this.disabled})}>
        <svg
          class="radar"
          viewBox="-130 -124 260 248"
          aria-label="Soap quality radar chart"
        >
          ${d} ${h}
          ${l.length?At`<polygon class="preferred" points=${Vo(l)}><title>Preferred range</title></polygon>`:""}
          ${u.length?At`<polygon class="reachable" points=${Vo(u)}><title>Axis reach</title></polygon>`:""}
          <polygon class="current" points=${Vo(o)}></polygon>
          ${i.length?At`<polygon class="preview" points=${Vo(i)}></polygon>`:""}
          ${E} ${p} ${g}
        </svg>
        <div class="after-chart">
          <slot name="after-chart"></slot>
        </div>
      </div>
    `}_patchRadarState(n){Object.assign(this,n)}_setRadarState(n){let r={axes:n.axes??[],value:pe(n.value??{}),previewValue:n.previewValue?pe(n.previewValue):null,targetValue:n.targetValue?pe(n.targetValue):null,reachableRange:n.reachableRange??{},preferredRange:n.preferredRange??{},selectedKey:n.selectedKey??"",hits:n.hits??[],disabled:!!n.disabled,precision:n.precision==null?null:Number(n.precision),showValues:!!n.showValues,theme:n.theme??{}};this.axes=r.axes,this.value=r.value,this.previewValue=r.previewValue,this.targetValue=r.targetValue,this.reachableRange=r.reachableRange,this.preferredRange=r.preferredRange,this.selectedKey=r.selectedKey,this.hits=r.hits,this.disabled=r.disabled,this.precision=r.precision,this.showValues=r.showValues,this.theme=r.theme}_select(n){this.disabled||(this.selectedKey=n,O(this,"vector-select",{source:"radar",role:"metric",transactionId:P("select"),key:n}))}_startDrag(n,r){this.disabled||n.button>0||(n.preventDefault(),this._select(r),this._drag={key:r,transactionId:P("radar"),startValue:pe(this.value),latest:null},this.pointerDrag.start({move:o=>this._dragMove(o),end:()=>this._dragEnd(),cancel:()=>this._cancelDrag()}))}_dragMove(n){if(!this._drag)return;let r=this._valueFromPoint(n,this._drag.key);this._previewMetric(r)}_dragEnd(){this._drag&&(this._drag.latest&&O(this,"vector-change",this._drag.latest),this._drag=null)}_cancelDrag(){this._drag&&(O(this,"vector-cancel",{source:"radar",role:"metric",transactionId:this._drag.transactionId,previousValue:this._drag.startValue}),this._drag=null)}_keydown(n,r){let o=this._axis(r);if(!o)return;let i=Number(o.step??1),s=null;if(n.key==="ArrowUp"||n.key==="ArrowRight"?s=Number(this.value[r]??0)+i:n.key==="ArrowDown"||n.key==="ArrowLeft"?s=Number(this.value[r]??0)-i:n.key==="Home"?s=Number(o.min??0):n.key==="End"?s=Number(o.max??100):n.key==="Escape"&&(this._cancelDrag(),O(this,"vector-cancel",{source:"radar",role:"metric",transactionId:P("radar-cancel"),previousValue:this.value})),s==null)return;n.preventDefault();let a=this._detailForMetric(r,s,P("radar-key"),this.value);O(this,"vector-preview",a),O(this,"vector-change",a)}_previewMetric(n){let r=this._drag;if(!r)return;let o=this._detailForMetric(r.key,n,r.transactionId,r.startValue);r.latest=o,O(this,"vector-preview",o)}_detailForMetric(n,r,o,i){let s=this._axis(n),a=T(m({},i),{[n]:Ce(K(r,s?.min??0,s?.max??100),s?.step??1)});return{source:"radar",role:"metric",transactionId:o,key:n,previousValue:i,requestedValue:a,proposedValue:a,delta:w(Number(a[n]??0)-Number(i[n]??0),4),hits:[]}}_valueFromPoint(n,r){let o=this.svgElement;if(!o)return Number(this.value[r]??0);let i=o.getBoundingClientRect(),s=(n.clientX-i.left)/i.width*260-130,a=(n.clientY-i.top)/i.height*248-124,c=this.axes.findIndex(g=>g.key===r),l=this.axes[c],u=$o(c,this.axes.length),d=Math.cos(u),h=Math.sin(u),p=K((s*d+a*h)/90,0,1);return Number(l?.min??0)+p*(Number(l?.max??100)-Number(l?.min??0))}_axis(n){return this.axes.find(r=>r.key===n)}_places(n){return Number.isFinite(this.precision)?Math.max(0,Number(this.precision)):Ao(n.step??1)}_pointsFor(n,r){return this.axes.map((o,i)=>vl(o,Number(n[o.key]??0),i,this.axes.length,r))}_pointsForRange(n,r){return this.axes.map((o,i)=>{let s=this.reachableRange[o.key],a=n==="reachableMax"?s?.max??o.reachableMax??o.max:this.preferredRange[o.key]?.max??o.preferredMax;return a==null?null:vl(o,Number(a),i,this.axes.length,r)}).filter(o=>o!==null)}};QE=new WeakMap,ZE=new WeakMap,YE=new WeakMap,JE=new WeakMap,XE=new WeakMap,ew=new WeakMap,tw=new WeakMap,nw=new WeakMap,rw=new WeakMap,ow=new WeakMap,iw=new WeakMap,sw=new WeakMap,aw=new WeakMap,v([fn("svg")],Ae.prototype,"svgElement",1),v([No()],Ae.prototype,"_drag",1),v([_({attribute:!1})],Ae.prototype,"axes",1),v([_({attribute:!1})],Ae.prototype,"value",1),v([_({attribute:!1})],Ae.prototype,"previewValue",1),v([_({attribute:!1})],Ae.prototype,"targetValue",1),v([_({attribute:!1})],Ae.prototype,"reachableRange",1),v([_({attribute:!1})],Ae.prototype,"preferredRange",1),v([_({attribute:!1})],Ae.prototype,"selectedKey",1),v([_({attribute:!1})],Ae.prototype,"hits",1),v([_({attribute:!1})],Ae.prototype,"disabled",1),v([_({attribute:!1})],Ae.prototype,"precision",1),v([_({attribute:!1})],Ae.prototype,"showValues",1)});var cw,lw=f(()=>{"use strict";W();cw=Z`:host {
  display: block;
}`});var ws,Vp=f(()=>{"use strict";W();nt();He();lw();ws=class extends te{static{this.styles=cw}#e={};get state(){return this.#e}set state(e){this.#e=e}render(){return C`<vector-constraint-rail
      .item=${this.state.item??null}
      .value=${this.state.value??0}
      .constraint=${this.state.constraint??{}}
      .role=${this.state.role??"composition"}
      .hits=${this.state.hits??[]}
      .rangeMin=${this.state.rangeMin??0}
      .rangeMax=${this.state.rangeMax??100}
      .precision=${this.state.precision??null}
      .defaultValue=${this.state.defaultValue??null}
      .thresholds=${this.state.thresholds??[]}
      .unitOptions=${this.state.unitOptions??[]}
      .unitKey=${this.state.unitKey??""}
      .measurementSystem=${this.state.measurementSystem??""}
      .basis=${this.state.basis??""}
      .measurementScope=${this.state.measurementScope??""}
      .measurementMeasure=${this.state.measurementMeasure??""}
      .context=${this.state.context??null}
      .disabled=${this.state.disabled??!1}
      .palette=${this.state.palette}
      .variant=${this.state.variant??"default"}
      .unitDisplay=${this.state.unitDisplay??"picker"}
      .activeEmphasis=${this.state.activeEmphasis??!0}
    ></vector-constraint-rail>`}};v([_({attribute:!1})],ws.prototype,"state",1)});var uw,dw=f(()=>{"use strict";W();uw=Z`:host {
  display: block;
}

.summary-meter {
  display: grid;
  gap: 6px;
}

.summary-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(82px, 0.8fr) auto;
  gap: 8px;
  align-items: center;
  min-height: 34px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  color: var(--_vector-ink);
  padding: 6px 8px;
  text-align: left;
  cursor: pointer;
}

.summary-row.warn {
  border-color: color-mix(in srgb, var(--_vector-warning) 55%, var(--_vector-line));
}

.summary-row.danger {
  border-color: color-mix(in srgb, var(--_vector-danger) 55%, var(--_vector-line));
  color: var(--_vector-danger);
}

.summary-track {
  position: relative;
  block-size: 7px;
  border-radius: 999px;
  background: var(--_vector-track);
  overflow: hidden;
}

.summary-track span {
  position: absolute;
  inset-block: 0;
  left: 0;
  border-radius: inherit;
  background: var(--_vector-accent-strong);
  width: var(--meter-fill);
}

.warn .summary-track span {
  background: var(--_vector-warning);
}

.danger .summary-track span {
  background: var(--_vector-danger);
}

strong {
  font-size: 0.78rem;
}

.quiet {
  min-height: 34px;
  border: 1px dashed var(--_vector-line);
  border-radius: 8px;
  color: var(--_vector-muted);
  padding: 8px 10px;
}`});var Is,Fp=f(()=>{"use strict";W();nt();wt();Ot();pn();dw();He();hn();xe();Ue();Is=class extends te{static{this.styles=[se,uw]}#e;get items(){return this.#e}set items(e){this.#e=e}constructor(){super(),this.items=[],this.theme={}}render(){return C`
      <div class="summary-meter">
        ${this.items.length?ue(this.items,e=>e.key,e=>{let n=W_(e.value,e.thresholds??[]),r=Number(e.min??0),o=Number(e.max??100),i=K((Number(e.value??0)-r)/(o-r||1)*100,0,100);return C`
                  <button
                    type="button"
                    class=${G({"summary-row":!0,[n?.severity||e.severity||"ok"]:!0})}
                    data-key=${e.key}
                    @click=${()=>this._select(e,n)}
                  >
                    <span>${e.label||Q(e.key)}</span>
                    <span class="summary-track"
                      ><span
                        ${de({"--meter-fill":`${i}%`})}
                      ></span
                    ></span>
                    <strong
                      >${Be(e.value,e.precision??1)}${e.unit||""}</strong
                    >
                  </button>
                `}):C`<div class="quiet">No summary values.</div>`}
      </div>
    `}_select(e,n){O(this,"vector-select",{source:"table",role:"summary",transactionId:P("summary"),key:e.key,thresholdKey:n?.key})}};v([_({attribute:!1})],Is.prototype,"items",1)});var fw,pw=f(()=>{"use strict";W();fw=Z`:host {
  display: block;
}

.unit-toggle {
  display: grid;
  gap: 5px;
}

.toggle-label {
  color: var(--_vector-muted);
  font-size: 0.72rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0;
}

.toggle-options {
  display: inline-grid;
  grid-auto-flow: column;
  grid-auto-columns: minmax(0, 1fr);
  gap: 0;
  padding: 3px;
  border: 1px solid var(--_vector-line);
  border-radius: 999px;
  background: var(--_vector-surface-strong);
}

button {
  min-height: 30px;
  border: 0;
  border-radius: 999px;
  background: transparent;
  color: var(--_vector-ink);
  padding: 0 11px;
  cursor: pointer;
  white-space: nowrap;
}

button.selected {
  background: var(--_vector-surface);
  color: var(--_vector-accent-strong);
  font-weight: 700;
  box-shadow: 0 1px 4px color-mix(in srgb, var(--_vector-ink) 14%, transparent);
}

button:disabled {
  opacity: 0.58;
  cursor: default;
}`});var ft,jp=f(()=>{"use strict";W();nt();wt();Ot();pw();He();xe();Ue();ft=class extends te{static{this.styles=[se,fw]}#e;get options(){return this.#e}set options(e){this.#e=e}#t;get unitKey(){return this.#t}set unitKey(e){this.#t=e}#n;get label(){return this.#n}set label(e){this.#n=e}#r;get groupKey(){return this.#r}set groupKey(e){this.#r=e}#o;get measurementSystem(){return this.#o}set measurementSystem(e){this.#o=e}#i;get basis(){return this.#i}set basis(e){this.#i=e}#s;get measurementScope(){return this.#s}set measurementScope(e){this.#s=e}#a;get measurementMeasure(){return this.#a}set measurementMeasure(e){this.#a=e}#c;get disabled(){return this.#c}set disabled(e){this.#c=e}constructor(){super(),this.options=[],this.unitKey="",this.label="",this.groupKey="",this.measurementSystem="",this.basis="",this.measurementScope="",this.measurementMeasure="",this.disabled=!1,this.theme={}}set state(e){let n=e??{};this.options=n.options??n.unitOptions??[],this.unitKey=n.unitKey??"",this.label=n.label??"",this.groupKey=n.groupKey??n.key??"",this.measurementSystem=n.measurementSystem??"",this.basis=n.basis??"",this.measurementScope=n.measurementScope??"",this.measurementMeasure=n.measurementMeasure??"",this.disabled=n.disabled??!1,this.theme=n.theme??{}}get state(){return{options:this.options,unitKey:this.unitKey,label:this.label,groupKey:this.groupKey,measurementSystem:this.measurementSystem,basis:this.basis,measurementScope:this.measurementScope,measurementMeasure:this.measurementMeasure,disabled:this.disabled,theme:this.theme}}render(){return C`
      <div
        class=${G({"unit-toggle":!0,binary:this.options.length===2})}
        role="group"
        aria-label=${this.label||"Measurement units"}
      >
        ${this.label?C`<span class="toggle-label">${this.label}</span>`:ie}
        <span class="toggle-options">
          ${ue(this.options,e=>e.key,e=>{let n=this.unitKey?e.key===this.unitKey:e.system?e.system===this.measurementSystem:e.basis===this.basis||e.scope===this.measurementScope||e.measure===this.measurementMeasure;return C`
                <button
                  type="button"
                  class=${G({selected:n})}
                  ?disabled=${this.disabled}
                  aria-pressed=${String(!!n)}
                  @click=${()=>this._select(e)}
                >
                  ${e.label}
                </button>
              `})}
        </span>
      </div>
    `}_select(e){O(this,"vector-change",{source:"inspector",role:"measurement",transactionId:P("unit"),key:this.groupKey,unitKey:e.key,basis:e.basis,measurementScope:e.scope||this.measurementScope,measurementMeasure:e.measure||this.measurementMeasure,measurementSystem:e.system||this.measurementSystem})}};v([_({attribute:!1})],ft.prototype,"options",1),v([_({attribute:!1})],ft.prototype,"unitKey",1),v([_({attribute:!1})],ft.prototype,"label",1),v([_({attribute:!1})],ft.prototype,"groupKey",1),v([_({attribute:!1})],ft.prototype,"measurementSystem",1),v([_({attribute:!1})],ft.prototype,"basis",1),v([_({attribute:!1})],ft.prototype,"measurementScope",1),v([_({attribute:!1})],ft.prototype,"measurementMeasure",1),v([_({attribute:!1})],ft.prototype,"disabled",1)});var hw,mw=f(()=>{"use strict";W();hw=Z`:host {
  display: block;
}

* {
  box-sizing: border-box;
}

.empty,
.value-slider {
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  padding: 10px 12px;
  background: var(--_vector-surface-alt);
}

.empty {
  color: var(--_vector-muted);
}

.value-slider {
  display: grid;
  gap: 8px;
}

.slider-head {
  display: grid;
  grid-template-columns: 12px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
}

.swatch {
  inline-size: 12px;
  block-size: 12px;
  border-radius: 999px;
  background: var(--item-color);
}

.slider-copy {
  display: grid;
  gap: 2px;
  min-width: 0;
}

label {
  color: var(--_vector-ink);
  font-size: 0.84rem;
  font-weight: 600;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

small {
  color: var(--_vector-muted);
  font-size: 0.72rem;
  line-height: 1.25;
}

.slider-value {
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 5px;
  min-width: 0;
  color: var(--_vector-ink);
  font-size: 0.82rem;
  font-weight: 600;
  text-align: right;
}

.value-slider.active label,
.value-slider.active .slider-value {
  font-weight: 850;
}

output {
  color: inherit;
  font: inherit;
}

.unit-text {
  color: var(--_vector-muted);
  font-size: 0.76rem;
  font-weight: 600;
}

.slider-track {
  position: relative;
  min-height: 30px;
  display: grid;
  align-items: center;
}

.full-track,
.threshold-track {
  position: absolute;
  left: 0;
  right: 0;
  top: 11px;
  block-size: 8px;
  border-radius: 999px;
}

.full-track {
  background: var(--_vector-track);
}

.threshold-track {
  overflow: hidden;
}

.threshold-band {
  position: absolute;
  top: 0;
  bottom: 0;
  left: var(--threshold-start);
  width: var(--threshold-width);
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-accent-strong) 35%, transparent));
}

.threshold-band.ok {
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-ok) 42%, transparent));
}

.threshold-band.warn {
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-warning) 46%, transparent));
}

.threshold-band.danger {
  background: var(--threshold-color, color-mix(in srgb, var(--_vector-danger) 48%, transparent));
}

.threshold-band.active {
  box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--_vector-ink) 30%, transparent);
}

.current-marker {
  position: absolute;
  top: 4px;
  inline-size: 4px;
  block-size: 22px;
  margin-left: -2px;
  border-radius: 999px;
  background: var(--_vector-accent-strong);
  pointer-events: none;
  z-index: 1;
  left: var(--marker-position);
}

input[type=range] {
  position: relative;
  z-index: 2;
  width: 100%;
  opacity: 0.02;
  cursor: ew-resize;
}

.unit-picker {
  position: relative;
}

.unit-picker summary {
  list-style: none;
  cursor: pointer;
}

.unit-picker summary::-webkit-details-marker {
  display: none;
}

.unit-pill,
.unit-picker summary {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  max-width: 92px;
  border: 1px solid var(--_vector-line);
  border-radius: 999px;
  background: var(--_vector-surface);
  color: var(--_vector-accent-strong);
  padding: 0 8px;
  font-size: 0.7rem;
  font-weight: 800;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.unit-pill.static {
  color: var(--_vector-muted);
}

.unit-picker[open] summary {
  border-color: var(--_vector-accent-strong);
  background: var(--_vector-accent-soft);
}

.unit-menu {
  position: absolute;
  right: 0;
  top: calc(100% + 4px);
  z-index: 5;
  display: grid;
  gap: 3px;
  min-width: 120px;
  padding: 4px;
  border: 1px solid var(--_vector-line);
  border-radius: 8px;
  background: var(--_vector-surface);
  box-shadow: 0 8px 20px color-mix(in srgb, var(--_vector-ink) 14%, transparent);
}

.unit-menu button {
  min-height: 28px;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: var(--_vector-ink);
  padding: 0 8px;
  text-align: left;
  font-size: 0.76rem;
  cursor: pointer;
}

.unit-menu button:hover,
.unit-menu button:focus {
  background: var(--_vector-surface-strong);
  outline: none;
}

.unit-menu button.selected {
  background: var(--_vector-accent-soft);
  color: var(--_vector-accent-strong);
  font-weight: 800;
}

input:disabled,
.disabled .unit-picker summary {
  opacity: 0.58;
  cursor: default;
}

.disabled {
  opacity: 0.65;
  pointer-events: none;
}`});var gw,vw,Ss,Up=f(()=>{"use strict";W();nt();wt();pn();mw();hn();xe();Ue();pl();He();Np();Ss=class extends te{constructor(){super();N(this,gw,"composition");this.scalar=new Fn(this);N(this,vw);Po(this)}static{this.properties=Oo}get role(){return M(this,gw)}set role(n){R(this,gw,n)}get sliderElement(){return M(this,vw)}set sliderElement(n){R(this,vw,n)}static{this.styles=[se,hw]}set state(n){this.scalar.setState(n)}get state(){return this.scalar.state}render(){if(!this.item)return C` <div class="empty">Select an item.</div> `;let n=this.scalar.viewState(),r=n.valueText,o=n.unit,i=this.scalar.unitOption(),s=this.item.description||this.item.summary||this.item.purpose||"",a=this.activeEmphasis&&Math.abs(Number(this.value||0))>1e-9,c=()=>this.sliderElement?.dataset.transaction||P("value-slider");return C`
      <section
        class=${G({"value-slider":!0,active:a,disabled:this.disabled})}
        data-threshold=${n.thresholdKey||""}
      >
        <div class="slider-head">
          <span
            class="swatch"
            ${de({"--item-color":dt(this.item,0,this.palette)})}
          ></span>
          <div class="slider-copy">
            <label for="slider"
              >${this.item.label||Q(this.item.key)}</label
            >
            ${s?C`<small>${s}</small>`:ie}
          </div>
          <span class="slider-value">
            <output>${r}</output>
            ${this._renderUnit(i,o)}
          </span>
        </div>
        <div class="slider-track">
          <div class="full-track"></div>
          <div class="threshold-track" aria-hidden="true">
            ${hl({thresholds:this.thresholds,value:this.value,percent:l=>this.scalar.pct(l),activeThreshold:l=>this.scalar.thresholdFor(l)})}
          </div>
          <span
            class="current-marker"
            ${de({"--marker-position":`${n.valuePct}%`})}
          ></span>
          <input
            id="slider"
            data-slider
            type="range"
            min=${n.domainMinText}
            max=${n.domainMaxText}
            step=${n.displayStep}
            .value=${r}
            ?disabled=${this.disabled}
            @input=${l=>{let u=l.currentTarget;u.dataset.transaction||=P("value-slider"),this._emitSliderValue(Number(u.value),"vector-preview",u.dataset.transaction??c())}}
            @change=${l=>{let u=l.currentTarget,d=u.dataset.transaction??c();delete u.dataset.transaction,this._emitSliderValue(Number(u.value),"vector-change",d)}}
            @keydown=${l=>this._sliderKeydown(l)}
          />
        </div>
      </section>
    `}_renderUnit(n,r){let o=this._unitPickerOptions(),i=Cp(n,r);return this.unitDisplay==="text"?C`<span
        class="unit-text"
        data-unit
        data-unit-key=${n?.key||""}
        title=${n?.label||i}
        >${i}</span
      >`:o.length<=1?C`<span
        class="unit-pill static"
        data-unit
        data-unit-key=${n?.key||""}
        title=${n?.label||i}
        >${i}</span
      >`:C`
      <details class="unit-picker">
        <summary
          data-unit
          data-unit-pill
          data-unit-key=${n?.key||""}
          aria-label=${`Change unit, currently ${n?.label||i}`}
          title=${`Change unit, currently ${n?.label||i}`}
        >
          ${i}
        </summary>
        <div class="unit-menu" role="listbox" aria-label="Units">
          ${o.map(s=>C`
              <button
                type="button"
                class=${G({selected:s.key===n?.key})}
                data-unit-option=${s.key}
                role="option"
                aria-selected=${String(s.key===n?.key)}
                @click=${a=>{a.preventDefault(),this.scalar.changeUnit(s.key),this._closeUnitPicker(a.currentTarget)}}
              >
                ${s.label||Cp(s,s.symbol)}
              </button>
            `)}
        </div>
      </details>
    `}_unitPickerOptions(){return this.unitOptions}_closeUnitPicker(n){let r=n.closest("details");r&&(r.open=!1)}_emitSliderValue(n,r,o){if(!this.item?.key)return;let i=this.scalar.unitOption(),s=this.scalar.toCanonical(n),a=Ce(s,this.scalar.step()),c=K(a,this.scalar.min(),this.scalar.max()),l=this.scalar.thresholdFor(c);O(this,r,{source:"inspector",role:this.role,transactionId:o,key:this.item.key,previousValue:{[this.item.key]:this.value},requestedValue:{[this.item.key]:a},proposedValue:{[this.item.key]:c},delta:w(c-this.value,4),unitKey:i?.key,basis:i?.basis||this.basis||this.item?.basis,measurementScope:i?.scope||this.measurementScope,measurementMeasure:i?.measure||this.measurementMeasure,displayValue:this.scalar.toDisplay(c),canonicalValue:c,thresholdKey:l?.key,measurementSystem:this.measurementSystem||i?.system,hits:[]})}_sliderKeydown(n){if(n.key!=="Escape")return;let r=n.currentTarget,o=r.dataset.transaction??P("value-slider-cancel");delete r.dataset.transaction,O(this,"vector-cancel",{source:"inspector",role:this.role,transactionId:o,key:this.item?.key})}};gw=new WeakMap,vw=new WeakMap,v([fn("[data-slider]")],Ss.prototype,"sliderElement",1)});function yw(){ke("vector-pie-chart",we),ke("vector-radar-chart",Ae),ke("vector-constraint-control",$n),ke("vector-constraint-rail",Lo),ke("vector-scalar-rail",ws),ke("vector-value-slider",Ss),ke("vector-quantity-input",bl),ke("vector-unit-toggle",ft),ke("vector-item-list",ze),ke("vector-ingredient-list",ml),ke("vector-amount-list",Ee),ke("vector-choice-control",mn),ke("vector-summary-meter",Is),ke("oil-search-list",dl),ke("vector-inspector",gn),ke("constraint-feedback-list",vs)}var Hp=f(()=>{"use strict";wp();xp();Dp();Tp();He();Mp();Rp();kp();Ap();bs();Pp();Lp();$p();Vp();Fp();jp();Up()});var bw=f(()=>{"use strict"});var _w=f(()=>{"use strict";Hp();wp();pn();Hp();xp();Dp();Tp();qt();Mp();Rp();kp();Ap();bs();Pp();Lp();$p();Vp();Fp();Ue();bw();jp();Up();yw()});var _l=f(()=>{"use strict";_w();xe()});function zp(t){let e=t?.constants?.sap_values?.naoh;return e&&typeof e=="object"?e:{}}function Ew(t){let{oilWeightG:e,oilVector:n,formulaValues:r,additiveValues:o,selectedAdditives:i,additiveItems:s,workspace:a}=t,c=a?.constants?.density??{},l=zp(a),u=mN(n,c,.92),d=K(Number(r.superfat??5),0,100),h=K(Number(r.lye_concentration??33),1,99),p=Object.entries(n).reduce((V,[I,le])=>V+e*(le/100)*Number(l[I]??0)*(1-d/100),0),g=p*(100-h)/h,E=i.reduce((V,I)=>V+e*(Number(o[I]??0)/100),0),b=i.reduce((V,I)=>{let le=s.find(j=>j.key===I),ae=Number(c[le?.material??I]);return Number.isFinite(ae)&&ae>0?V+e*(Number(o[I]??0)/100)/ae:V},0),D=e/u+p/Bp(c,"naoh",2.13)+g/Bp(c,"distilled_water",1)+b;return{oilWeightG:e,lyeG:w(p,1),liquidG:w(g,1),totalMassG:w(e+p+g+E,1),volumeMl:w(D,1),volumeIn3:w(D/hN,2),oilDensity:w(u,4)}}function mN(t,e,n){let r=Ln(t)||100;return Object.entries(t).reduce((o,[i,s])=>o+s/r*Bp(e,i,n),0)||n}function Bp(t,e,n=1){let r=Number(t?.[e]??t?.[e.toLowerCase()]);return Number.isFinite(r)&&r>0?r:n}function Cs(t){return t&&typeof t=="object"?t:{}}function Nr(t){return Number(Cs(t).density??0)||1}function El(t,e){return t/100*Number(Cs(e).oilWeightG??0)}function wl(t,e){return t/Number(Cs(e).oilWeightG??1)*100}function qp(t,e){return El(t,e)/Nr(e)}function Wp(t,e){return wl(t*Nr(e),e)}function ww(t,e){let n=Cs(e),r=Number(n.oilWeightG??0);return t*r/Number(n.totalMassG??(r||1))}function Iw(t,e){let n=Cs(e),r=Number(n.oilWeightG??1);return t*Number(n.totalMassG??(r||1))/r}var hN,Sw=f(()=>{"use strict";_l();hN=16.387064});var Cw,xw=f(()=>{"use strict";W();Cw=Z`:host {
  display: block;
  color: var(--vector-ink);
}

* {
  box-sizing: border-box;
}

button,
input {
  font: inherit;
}

.workbench {
  display: grid;
  gap: 16px;
}

.toolbar,
.card-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.toolbar {
  padding: 4px 0;
}

h2,
h3,
p {
  margin: 0;
}

h2 {
  font-size: 1rem;
}

h3 {
  font-size: 0.95rem;
}

p,
.card-heading span {
  color: var(--vector-muted);
  font-size: 0.82rem;
}

.toolbar-actions {
  display: flex;
  gap: 8px;
}

button {
  min-height: 34px;
  border: 1px solid var(--vector-line);
  border-radius: 8px;
  background: var(--vector-surface);
  color: var(--vector-ink);
  padding: 0 12px;
  cursor: pointer;
}

button:disabled {
  opacity: 0.45;
  cursor: default;
}

button:hover:not(:disabled) {
  border-color: var(--vector-accent-strong);
}

.surface {
  border: 1px solid var(--vector-line);
  border-radius: 8px;
  background: var(--vector-surface);
  padding: 14px;
  min-width: 0;
}

.visual-grid {
  display: grid;
  grid-template-columns: minmax(320px, 1.1fr) minmax(320px, 1.1fr) minmax(240px, 0.8fr);
  gap: 14px;
  align-items: start;
}

.control-grid {
  display: grid;
  grid-template-columns: minmax(300px, 1fr) minmax(260px, 0.8fr) minmax(340px, 1fr);
  gap: 14px;
  align-items: start;
}

.side-stack {
  display: grid;
  gap: 14px;
}

.formula-stack,
.additive-stack {
  display: grid;
  gap: 10px;
}

.measurement-toggles {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  align-items: end;
}

.measurement-toggles.system-only {
  grid-template-columns: minmax(150px, 0.45fr);
}

.measurement-toggles.additive-toggles {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.batch-size-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.card-heading {
  margin-bottom: 12px;
}

.pie-lock-badge,
.pie-limit-badge {
  display: grid;
  place-items: center;
  inline-size: 22px;
  block-size: 22px;
  border-radius: 999px;
  background: var(--vector-surface);
  border: 1px solid var(--vector-line);
  box-shadow: 0 1px 4px color-mix(in srgb, var(--vector-ink) 18%, transparent);
  font-size: 0.72rem;
  font-weight: 800;
  line-height: 1;
}

.pie-lock-badge {
  color: var(--vector-accent-strong);
}

.pie-limit-badge {
  color: var(--vector-danger);
  border-color: color-mix(in srgb, var(--vector-danger) 55%, var(--vector-line));
}

.pie-limit-badge.soft {
  color: var(--vector-warning);
  border-color: color-mix(in srgb, var(--vector-warning) 65%, var(--vector-line));
}

.pie-lock-badge.hit,
.pie-limit-badge.hit {
  animation: badgePulse 700ms ease-in-out infinite alternate;
}

.quality-rail-list {
  display: grid;
  gap: 8px;
}

.quality-row {
  display: grid;
  grid-template-columns: minmax(96px, 0.8fr) minmax(150px, 1.4fr) 48px 70px;
  gap: 10px;
  align-items: center;
  min-height: 42px;
  padding: 7px 8px;
  border: 1px solid var(--vector-line);
  border-radius: 8px;
  background: var(--vector-surface-alt);
}

.quality-row.selected {
  border-color: var(--vector-accent-strong);
  background: var(--vector-accent-soft);
}

.quality-name {
  min-height: 28px;
  padding: 0;
  border: 0;
  background: transparent;
  color: var(--vector-ink);
  text-align: left;
  font-weight: 500;
}

.quality-row.selected .quality-name {
  color: var(--vector-accent-strong);
  font-weight: 800;
}

.quality-combo {
  position: relative;
  min-height: 28px;
  display: grid;
  align-items: center;
}

.quality-track {
  position: absolute;
  left: 0;
  right: 0;
  top: 10px;
  height: 8px;
  border-radius: 999px;
  background: var(--vector-track);
  overflow: hidden;
  pointer-events: none;
}

.quality-reach,
.quality-preferred {
  position: absolute;
  top: 0;
  bottom: 0;
  left: var(--quality-start);
  width: var(--quality-width);
  border-radius: 999px;
}

.quality-reach {
  background: color-mix(in srgb, var(--vector-reach) 20%, transparent);
}

.quality-preferred {
  background: color-mix(in srgb, var(--vector-accent-strong) 22%, transparent);
}

.quality-current {
  position: absolute;
  top: -4px;
  left: var(--quality-position);
  inline-size: 4px;
  block-size: 16px;
  margin-left: -2px;
  border-radius: 999px;
  background: var(--vector-accent-strong);
}

.quality-combo input {
  position: relative;
  z-index: 1;
  width: 100%;
  opacity: 0.01;
  cursor: ew-resize;
}

.quality-row output {
  font-weight: 600;
  text-align: right;
}

.quality-reach-label {
  color: var(--vector-muted);
  font-size: 0.78rem;
  text-align: right;
}

.empty {
  padding: 16px;
  color: var(--vector-muted);
  border: 1px dashed var(--vector-line);
  border-radius: 8px;
}

@keyframes badgePulse {
  from {
    transform: scale(1);
  }
  to {
    transform: scale(1.2);
  }
}
@media (max-width: 1180px) {
  .visual-grid,
  .control-grid {
    grid-template-columns: 1fr;
  }
  .toolbar {
    align-items: stretch;
    flex-direction: column;
  }
}
@media (max-width: 720px) {
  .measurement-toggles,
  .measurement-toggles.system-only,
  .measurement-toggles.additive-toggles,
  .batch-size-grid {
    grid-template-columns: 1fr;
  }
  .quality-row {
    grid-template-columns: 1fr 52px;
  }
  .quality-combo,
  .quality-reach-label {
    grid-column: 1/-1;
  }
}`});var jn,Gp,Dw,Tw,Mw,Rr,Kp=f(()=>{"use strict";jn={olive_oil:{lauric:0,myristic:0,palmitic:14,stearic:3,ricinoleic:0,oleic:69,linoleic:12,linolenic:1},coconut_oil:{lauric:48,myristic:19,palmitic:9,stearic:3,ricinoleic:0,oleic:8,linoleic:2,linolenic:0},lard:{lauric:0,myristic:1,palmitic:26,stearic:14,ricinoleic:0,oleic:44,linoleic:10,linolenic:1},castor_oil:{lauric:0,myristic:0,palmitic:2,stearic:1,ricinoleic:90,oleic:4,linoleic:4,linolenic:0},shea_butter:{lauric:0,myristic:0,palmitic:4,stearic:42,ricinoleic:0,oleic:46,linoleic:6,linolenic:0},cocoa_butter:{lauric:0,myristic:0,palmitic:26,stearic:34,ricinoleic:0,oleic:35,linoleic:3,linolenic:0},avocado_oil:{lauric:0,myristic:0,palmitic:20,stearic:1,ricinoleic:0,oleic:58,linoleic:12,linolenic:1},sweet_almond_oil:{lauric:0,myristic:0,palmitic:7,stearic:2,ricinoleic:0,oleic:68,linoleic:22,linolenic:0},sunflower_oil:{lauric:0,myristic:0,palmitic:6,stearic:4,ricinoleic:0,oleic:30,linoleic:59,linolenic:1},rice_bran_oil:{lauric:0,myristic:1,palmitic:22,stearic:2,ricinoleic:0,oleic:38,linoleic:34,linolenic:1},palm_oil:{lauric:0,myristic:1,palmitic:44,stearic:5,ricinoleic:0,oleic:39,linoleic:10,linolenic:0},jojoba_oil:{lauric:0,myristic:0,palmitic:2,stearic:1,ricinoleic:0,oleic:12,linoleic:5,linolenic:0}},Gp={olive_oil:"#6f8d42",coconut_oil:"#3f7f95",lard:"#b38b59",castor_oil:"#8b5f9d",shea_butter:"#c09654",cocoa_butter:"#8d5a3c",avocado_oil:"#4f9c68",sweet_almond_oil:"#c48a70",sunflower_oil:"#d6a13d",rice_bran_oil:"#9d9a5a",palm_oil:"#b66d4d",jojoba_oil:"#7fa89d"},Dw={olive_oil:.134,coconut_oil:.183,lard:.138,castor_oil:.128,shea_butter:.128,cocoa_butter:.137,avocado_oil:.133,sweet_almond_oil:.136,sunflower_oil:.135,rice_bran_oil:.128,palm_oil:.142,jojoba_oil:.069},Tw=1.4025,Mw={oleic:.899,linoleic:1.814,linolenic:2.737,ricinoleic:.86},Rr=[{key:"hardness",label:"Hardness",min:0,max:80,preferredMin:29,preferredMax:54,step:1},{key:"cleansing",label:"Cleansing",min:0,max:40,preferredMin:12,preferredMax:22,step:1},{key:"conditioning",label:"Conditioning",min:0,max:90,preferredMin:44,preferredMax:69,step:1},{key:"bubbly",label:"Bubbly",min:0,max:70,preferredMin:14,preferredMax:46,step:1},{key:"creamy",label:"Creamy",min:0,max:60,preferredMin:16,preferredMax:48,step:1}]});var Nw=f(()=>{"use strict"});function kw(t={}){let e=Object.keys(t.inventory?.raw_materials||{}),n=(t.base_recipes||[]).flatMap(i=>Object.keys(i.oils_pct||{})),r=Object.keys(jn);return[...new Set([...n,...e,...r])].filter(i=>i in jn).map((i,s)=>({key:i,label:Q(i),color:Gp[i]||EN(s),order:s,tags:_N(i)}))}function Aw(t={}){let n=((t.product_recipes||[])[0]||null)?.base_recipe_id||(t.base_recipes||[])[0]?.id,r=(t.base_recipes||[]).find(o=>o.id===n)||(t.base_recipes||[])[0];return Ep(r?.oils_pct||{olive_oil:35,coconut_oil:22,lard:36,castor_oil:7},100)}function Ow(t=[]){let e={olive_oil:{min:10,max:70,step:1},coconut_oil:{min:5,max:30,step:1},lard:{min:10,max:60,step:1},castor_oil:{min:5,max:10,step:1},shea_butter:{min:0,max:20,step:1},cocoa_butter:{min:0,max:15,step:1},avocado_oil:{min:0,max:30,step:1},sweet_almond_oil:{min:0,max:30,step:1},sunflower_oil:{min:0,max:30,step:1},rice_bran_oil:{min:0,max:40,step:1},palm_oil:{min:0,max:50,step:1},jojoba_oil:{min:0,max:10,step:1}};return Object.fromEntries(t.map(n=>[n,m({},e[n]||{min:0,max:100,step:1})]))}function Pt(t={},e=jn){let n=Lw(t,e);return{hardness:w(n.lauric+n.myristic+n.palmitic+n.stearic,1),cleansing:w(n.lauric+n.myristic,1),conditioning:w(n.oleic+n.linoleic+n.linolenic+n.ricinoleic,1),bubbly:w(n.lauric+n.myristic+n.ricinoleic,1),creamy:w(n.palmitic+n.stearic+n.ricinoleic,1)}}function Pw(t={},e={},n=jn){let r=Lw(t,n),o=Object.entries(Mw).reduce((s,[a,c])=>s+Number(r[a]||0)*c,0),i=Object.entries(t||{}).reduce((s,[a,c])=>{let l=Number(e?.[a]??Dw[a]??0);return s+Number(c||0)/100*l*Tw*1e3},0);return w(i-o,0)}function Lw(t={},e=jn){let n={lauric:0,myristic:0,palmitic:0,stearic:0,ricinoleic:0,oleic:0,linoleic:0,linolenic:0};for(let[r,o]of Object.entries(t||{})){let i=e[r];if(!i)continue;let s=Number(o||0)/100;for(let a of Object.keys(n))n[a]+=Number(i[a]||0)*s}return Object.fromEntries(Object.entries(n).map(([r,o])=>[r,w(o,2)]))}function $w({oilVector:t,key:e,target:n,oilConstraints:r={},oilItems:o=[]}){let i=Mr({value:t,key:e,target:n,constraints:r,items:o,total:100});return{oilVector:i.vector,qualityVector:Pt(i.vector),hits:i.hits,explanations:i.hits.map(s=>s.message)}}function Fo({oilVector:t,targetQuality:e,oilConstraints:n={},axes:r=Rr,profiles:o=jn}){let i=Object.keys(t||{}),s=Tr(gN(t,n),n,100,i[0]),a=Pt(s,o),c=Rw(s,a,e,t),l=yN(n,i);for(let d of l){let h=!0,p=0;for(;h&&p<250;){h=!1,p+=1;let g=s,E=a,b=c;for(let D of i)for(let V of i){if(D===V)continue;let I=vN(s,D,V,d,n);if(!I)continue;let le=Pt(I,o),ae=Rw(I,le,e,t);ae+1e-7<b&&(g=I,E=le,b=ae,h=!0)}s=g,a=E,c=b}}let u=bN(s,a,e,n,r);return{oilVector:s,qualityVector:a,hits:u,explanations:u.map(d=>d.message),score:c}}function Un(t,e={},n=Rr){let r={};for(let o of n){let i=Fo({oilVector:t,oilConstraints:e,axes:n,targetQuality:{[o.key]:o.min}}),s=Fo({oilVector:t,oilConstraints:e,axes:n,targetQuality:{[o.key]:o.max}});r[o.key]={min:w(i.qualityVector[o.key],1),max:w(s.qualityVector[o.key],1)}}return r}function gN(t={},e={}){let n=pe(t);for(let[r,o]of Object.entries(n)){let i=e[r]||{};n[r]=K(o,i.min??0,i.max??100)}return Tr(ll(n,e),e,100,Object.keys(n)[0])}function Vw(t,e,n,r,o=[]){let i=T(m({},e),{[n]:m(m({},e[n]||{}),r)}),s=pe(t),a=Number(s[n]||0),c=Number(i[n].min??0),l=Number(i[n].max??100);return(a<c||a>l)&&(s=Mr({value:s,key:n,target:K(a,c,l),constraints:i,items:o,total:100}).vector),{oilVector:s,oilConstraints:i,qualityVector:Pt(s)}}function vN(t,e,n,r,o){let i=o[e]||{},s=o[n]||{};if(i.locked||s.locked)return null;let a=Number(i.min??0),c=Number(s.max??100);if(Number(t[e]||0)-r<a-1e-7||Number(t[n]||0)+r>c+1e-7)return null;let l=pe(t);return l[e]=w(l[e]-r,4),l[n]=w(l[n]+r,4),Tr(ll(l,o),o,100,n)}function yN(t={},e=[]){let n=e.reduce((o,i)=>{let s=Number(t[i]?.step||1);return!Number.isFinite(s)||s<=0?o:Math.min(o,s)},1/0),r=Number.isFinite(n)?n:1;return[5,1,.25,.1].filter(o=>o>=r-1e-7)}function Rw(t,e,n={},r={}){return Object.entries(n).reduce((i,[s,a])=>{let c=Number(e[s]||0)-Number(a||0);return i+c*c},0)+ul(t,r)*.015}function bN(t,e,n,r,o){let i=[];for(let[s,a]of Object.entries(n)){let c=Number(e[s]||0);if(Math.abs(c-Number(a))>.6){let l=o.find(u=>u.key===s);i.push(he(s,"unreachable",`${l?.label||Q(s)} reached ${w(c,1)} instead of ${w(a,1)} with the selected oils.`))}}for(let[s,a]of Object.entries(t||{})){let c=r[s]||{};if(c.locked){i.push(he(s,"locked",`${Q(s)} is locked.`));continue}c.min!=null&&Math.abs(Number(a)-Number(c.min))<.05&&i.push(he(s,"min",`${Q(s)} hit minimum ${w(c.min,2)}%.`)),c.max!=null&&Math.abs(Number(a)-Number(c.max))<.05&&i.push(he(s,"max",`${Q(s)} hit maximum ${w(c.max,2)}%.`))}return Math.abs(Ln(t)-100)>.01&&i.push(he("composition","sum","Oil percentages must add to 100%.")),i}function _N(t){let e=new Set(["coconut_oil","lard","shea_butter","cocoa_butter","palm_oil"]),n=new Set(["olive_oil","avocado_oil","sweet_almond_oil","sunflower_oil","rice_bran_oil","jojoba_oil"]),r=new Set(["coconut_oil","castor_oil"]);return[e.has(t)?"hard":null,n.has(t)?"liquid":null,r.has(t)?"lather":null].filter(o=>o!==null)}function EN(t){let e=["#426a8c","#d9824b","#6f8d42","#8b5f9d","#9d9a5a","#c48a70"];return e[t%e.length]}var Fw=f(()=>{"use strict";xe();Kp();Nw();Kp()});function WN(){globalThis.customElements&&!customElements.get("recipe-vector-workbench")&&customElements.define("recipe-vector-workbench",Zp)}var wN,IN,SN,Sl,Cl,xl,jw,Qp,CN,xN,DN,TN,MN,NN,RN,kN,AN,ON,PN,LN,$N,VN,FN,jN,UN,HN,BN,rt,Wt,zN,qN,Il,Uw,Zp,Hw=f(()=>{"use strict";W();wt();Ot();_l();Sw();xw();Fw();_l();wN=String.fromCodePoint(128274),IN=String.fromCodePoint(8805),SN=String.fromCodePoint(8804),Sl=28.3495,Cl=453.592,xl=29.5735,jw=16.387064,Qp={key:"percent_oils",label:"% oils",basis:"percent_oils",scope:"oils",measure:"scalar",symbol:"%",precision:1,step:.1},CN={key:"percent_batch",label:"% batch",basis:"percent_batch",scope:"batch",measure:"scalar",symbol:"%",precision:1,step:.1,fromCanonical:(t,e)=>ww(t,e),toCanonical:(t,e)=>Iw(t,e)},xN={key:"g_per_kg_oils",label:"g/kg oils",system:"metric",basis:"weight",scope:"oils",measure:"weight",symbol:" g/kg",precision:0,step:1,fromCanonical:t=>Number(t||0)*10,toCanonical:t=>Number(t||0)/10},DN={key:"oz_per_lb_oils",label:"oz/lb oils",system:"imperial",basis:"weight",scope:"oils",measure:"weight",symbol:" oz/lb",precision:2,step:.05,fromCanonical:t=>Number(t||0)*.16,toCanonical:t=>Number(t||0)/.16},TN={key:"g_batch",label:"g",system:"metric",basis:"weight",scope:"batch",measure:"weight",symbol:" g",precision:1,step:1,fromCanonical:(t,e)=>El(t,e),toCanonical:(t,e)=>wl(t,e)},MN={key:"oz_batch",label:"oz",system:"imperial",basis:"weight",scope:"batch",measure:"weight",symbol:" oz",precision:2,step:.05,fromCanonical:(t,e)=>El(t,e)/Sl,toCanonical:(t,e)=>wl(Number(t||0)*Sl,e)},NN={key:"ml_per_kg_oils",label:"ml/kg oils",system:"metric",basis:"volume",scope:"oils",measure:"volume",symbol:" ml/kg",precision:0,step:1,fromCanonical:(t,e)=>Number(t||0)*10/Nr(e),toCanonical:(t,e)=>Number(t||0)*Nr(e)/10},RN={key:"ml_batch",label:"ml",system:"metric",basis:"volume",scope:"batch",measure:"volume",symbol:" ml",precision:1,step:1,fromCanonical:(t,e)=>qp(t,e),toCanonical:(t,e)=>Wp(t,e)},kN={key:"floz_per_lb_oils",label:"fl oz/lb oils",system:"imperial",basis:"volume",scope:"oils",measure:"volume",symbol:" fl oz/lb",precision:2,step:.05,fromCanonical:(t,e)=>Number(t||0)/100*Cl/Nr(e)/xl,toCanonical:(t,e)=>Number(t||0)*xl*Nr(e)/Cl*100},AN={key:"floz_batch",label:"fl oz",system:"imperial",basis:"volume",scope:"batch",measure:"volume",symbol:" fl oz",precision:2,step:.05,fromCanonical:(t,e)=>qp(t,e)/xl,toCanonical:(t,e)=>Wp(Number(t||0)*xl,e)},ON=[{key:"metric",label:"Metric",system:"metric",symbol:"g"},{key:"imperial",label:"Imperial",system:"imperial",symbol:"oz"}],PN=[{key:"oils",label:"Oils",scope:"oils"},{key:"batch",label:"Total",scope:"batch"}],LN=[{key:"weight",label:"Weight",measure:"weight"},{key:"volume",label:"Volume",measure:"volume"}],$N={key:"g",label:"g",system:"metric",basis:"weight",measure:"weight",symbol:" g",precision:0,step:25},VN={key:"kg",label:"kg",system:"metric",basis:"weight",measure:"weight",symbol:" kg",precision:2,step:.05,fromCanonical:t=>Number(t||0)/1e3,toCanonical:t=>Number(t||0)*1e3},FN={key:"oz",label:"oz",system:"imperial",basis:"weight",measure:"weight",symbol:" oz",precision:1,step:1,fromCanonical:t=>Number(t||0)/Sl,toCanonical:t=>Number(t||0)*Sl},jN={key:"lb",label:"lb",system:"imperial",basis:"weight",measure:"weight",symbol:" lb",precision:2,step:.1,fromCanonical:t=>Number(t||0)/Cl,toCanonical:t=>Number(t||0)*Cl},UN={key:"ml",label:"ml",system:"metric",basis:"volume",measure:"volume",symbol:" ml",precision:0,step:50},HN={key:"l",label:"L",system:"metric",basis:"volume",measure:"volume",symbol:" L",precision:2,step:.05,fromCanonical:t=>Number(t||0)/1e3,toCanonical:t=>Number(t||0)*1e3},BN={key:"in3",label:"in3",system:"imperial",basis:"volume",measure:"volume",symbol:" in3",precision:1,step:1,fromCanonical:t=>Number(t||0)/jw,toCanonical:t=>Number(t||0)*jw},rt=[{key:"oil_weight_g",label:"Oil weight",defaultValue:1800,min:250,max:1e4,step:25,defaultUnitKey:"g",unitOptions:[$N,VN,FN,jN]},{key:"batch_volume_ml",label:"Batch volume",defaultValue:3e3,min:400,max:16e3,step:25,defaultUnitKey:"ml",unitOptions:[UN,HN,BN]}],Wt=[{key:"superfat",label:"Superfat",unit:"%",defaultValue:5,min:0,max:15,step:.5,defaultUnitKey:"percent_oils",unitOptions:[Qp],thresholds:[{key:"lean",min:0,max:2.9,severity:"warn",label:"Low superfat can feel lean."},{key:"balanced",min:3,max:7,severity:"ok",label:"Balanced superfat range."},{key:"rich",min:7.1,max:10,severity:"warn",label:"Higher superfat can soften bars."},{key:"very_rich",min:10.1,max:15,severity:"danger",label:"Very high superfat may shorten shelf life."}]},{key:"lye_concentration",label:"Lye concentration",unit:"%",defaultValue:33,min:25,max:45,step:.5,defaultUnitKey:"percent_oils",unitOptions:[Qp],thresholds:[{key:"loose",min:25,max:27.9,severity:"warn",label:"Extra water slows unmolding."},{key:"friendly",min:28,max:35,severity:"ok",label:"Friendly cold-process range."},{key:"fast",min:35.1,max:40,severity:"warn",label:"Higher concentration can speed trace."},{key:"advanced",min:40.1,max:45,severity:"danger",label:"Advanced lye concentration."}]}],zN={key:"lye_type",label:"Lye type"},qN=[{key:"naoh",label:"NaOH"},{key:"koh",label:"KOH"},{key:"blend",label:"Blend"}],Il=[Qp,CN,xN,TN,DN,MN,NN,RN,kN,AN],Uw=[{key:"fragrance",label:"Fragrance",summary:"Scent load for the finished bar.",type:"fragrance",category:"scent",material:"fragrance_oil",color:"#8b5f9d",defaultValue:3.1,min:0,max:6,step:.1,defaultUnitKey:"percent_oils",unitOptions:Il,tags:["scent","ifra"],thresholds:[{key:"light",min:0,max:2.9,severity:"ok",label:"Light scent load."},{key:"standard",min:3,max:4.9,severity:"ok",label:"Typical scent load."},{key:"high",min:5,max:5.7,severity:"warn",label:"High scent load; check acceleration."},{key:"ifra_limit",min:5.8,max:6,severity:"danger",label:"Near supplier or IFRA maximum."}]},{key:"kaolin_clay",label:"Kaolin clay",summary:"Adds slip and can anchor scent.",type:"clay",category:"additive",material:"kaolin_clay",color:"#9d9a5a",defaultValue:.6,min:0,max:2,step:.1,defaultUnitKey:"percent_oils",unitOptions:Il,tags:["clay","slip"],thresholds:[{key:"light",min:0,max:.7,severity:"ok",label:"Light clay load."},{key:"noticeable",min:.8,max:1.4,severity:"warn",label:"Clay may affect slip and trace."},{key:"heavy",min:1.5,max:2,severity:"danger",label:"Heavy clay can feel draggy."}]},{key:"sodium_lactate",label:"Sodium lactate",summary:"Helps harden bars for unmolding.",type:"hardener",category:"additive",material:"sodium_lactate",color:"#426a8c",defaultValue:2,min:0,max:3,step:.1,defaultUnitKey:"percent_oils",unitOptions:Il,tags:["hardener"],thresholds:[{key:"normal",min:0,max:2,severity:"ok",label:"Typical hardening support."},{key:"brittle",min:2.1,max:3,severity:"warn",label:"Too much may make bars brittle."}]},{key:"botanicals",label:"Botanicals",summary:"Visual texture or top decoration.",type:"botanical",category:"decoration",material:"botanicals",color:"#6f8d42",defaultValue:.2,min:0,max:1,step:.1,defaultUnitKey:"percent_oils",unitOptions:Il,tags:["decoration","texture"],thresholds:[{key:"sprinkle",min:0,max:.3,severity:"ok",label:"Light decoration."},{key:"scratch",min:.4,max:1,severity:"warn",label:"Botanicals can brown or scratch."}]}],Zp=class extends zt{static{this.styles=Cw}constructor(){super(),this._workspace=null,this._initialized=!1,this._oilItems=[],this._oilVector={},this._oilConstraints={},this._qualityAxes=Rr,this._qualityConstraints={},this._qualityVector={hardness:0,cleansing:0,conditioning:0,bubbly:0,creamy:0},this._reachableRange={hardness:{min:0,max:0},cleansing:{min:0,max:0},conditioning:{min:0,max:0},bubbly:{min:0,max:0},creamy:{min:0,max:0}},this._selectedOil="",this._selectedQuality="hardness",this._selectionRole="composition",this._preview=null,this._hits=[],this._history=[],this._future=[],this._theme=il,ko(this,this._theme),this._palette=_e,this._pendingPreviewDetail=null,this._previewFrame=0,this._measurementSystem="metric",this._measurementUnitKey="metric",this._measurementScope="oils",this._measurementScopeKey="oils",this._measurementMeasure="weight",this._measurementMeasureKey="weight",this._measurementBasis="weight",this._batchOilWeightG=1800,this._unitPreferences={},this._formulaValues=Object.fromEntries([...rt,...Wt].map(e=>[e.key,e.defaultValue??0])),this._formulaConstraints=Object.fromEntries([...rt,...Wt].map(e=>[e.key,{min:e.min,max:e.max,step:e.step}])),this._formulaChoices={lye_type:"naoh"},this._additiveItems=Uw,this._selectedAdditives=["fragrance","kaolin_clay","sodium_lactate"],this._additiveValues=this._initialAdditiveValues(),this._additiveConstraints=Object.fromEntries(Uw.map(e=>[e.key,{min:e.min,max:e.max,step:e.step}])),this._selectedAdditive="fragrance",this._scalarPreview=null}set workspace(e){this._workspace=e,this._initialized?this.requestUpdate():this._loadWorkspace(e||{})}get workspace(){return this._workspace}set theme(e){let n=this._theme;this._theme=m(m({},il),e||{}),ko(this,this._theme,n),this.requestUpdate()}get theme(){return this._theme}set palette(e){this._palette=Array.isArray(e)&&e.length?e:_e,this.requestUpdate()}get palette(){return this._palette}connectedCallback(){super.connectedCallback(),!this._initialized&&this._workspace?this._loadWorkspace(this._workspace||{}):this._initialized?this.requestUpdate():this.requestUpdate()}render(){if(!this._initialized)return C`<div class="empty">Loading recipe design tools.</div>`;let e=this._preview?.qualityVector||this._qualityVector,n=this._activeReachableRange(),r=this._preview?.oilVector||this._oilVector,o=this._activeOilConstraints(),i=this._activeFormulaValues(),s=this._activeAdditiveValues(),a=this._batchContext(r,i,s),c=rt.find(S=>S.key==="oil_weight_g")||rt[0],l=rt.find(S=>S.key==="batch_volume_ml")||rt[1],u=this._additiveItemsForContext(a),d=this._scalarHits(i,s),h=[...this._activeHits(),...d],p=this._selectedOilItems(),g=this._oilItems.find(S=>S.key===this._selectedOil),E=Object.fromEntries(Wt.map(S=>[S.key,S.thresholds||[]])),b=Object.fromEntries(u.map(S=>[S.key,S.thresholds||[]])),D=this._formulaSummary(i,s),V=this._activeMeasurementBasis(),I=Pw(r,zp(this._workspace)),le={items:p,value:r,constraints:o,total:100,selectedKey:this._selectedOil,hits:h,precision:0,centerValue:I,centerLabel:"INS",centerPrecision:0,theme:this._theme,palette:this._palette},ae={axes:this._qualityAxes.map(S=>T(m({},S),{reachableMin:n[S.key]?.min,reachableMax:n[S.key]?.max})),value:this._qualityVector,previewValue:this._preview?.qualityVector||null,targetValue:this._preview?.targetQuality||null,reachableRange:n,selectedKey:this._selectedQuality,hits:h,precision:1,showValues:!0,theme:this._theme},j={item:g||null,value:r[this._selectedOil]||0,constraint:o[this._selectedOil]||{},role:"composition",hits:h,rangeMin:0,rangeMax:100,precision:0,theme:this._theme,palette:this._palette,variant:"embedded"},Ze=new Set(Object.entries(o||{}).filter(([,S])=>S?.locked).map(([S])=>S)),kr=new Set(h.filter(S=>S.type==="locked").map(S=>S.key)),Lt=new Set(Object.keys(o||{})),$t=new Map(h.filter(S=>(S.type==="min"||S.type==="max")&&Lt.has(S.key)).map(S=>[JSON.stringify([S.type,S.key]),S])),Gt=[...[...Ze].map(S=>C`
          <span
            slot="slice-decorator"
            data-lock-badge
            data-key=${S}
            class=${G({"pie-lock-badge":!0,hit:kr.has(S)})}
            aria-hidden="true"
            title=${`${this._labelForKey(S)} locked`}
            >${wN}</span
          >
        `),...[...$t.values()].map(S=>C`
          <span
            slot="slice-decorator"
            data-limit-badge
            data-key=${S.key}
            data-limit-type=${S.type}
            class=${G({"pie-limit-badge":!0,[S.type]:!0,[S.severity||"hard"]:!0,hit:!0})}
            aria-hidden="true"
            title=${`${this._labelForKey(S.key)} ${S.type==="min"?"minimum":"maximum"} reached`}
          >
            ${S.type==="min"?IN:SN}
          </span>
        `)];return C`
      <section
        class="workbench"
        @vector-select=${S=>this._handleSelect(S.detail)}
        @vector-preview=${S=>this._handlePreview(S.detail)}
        @vector-change=${S=>this._handleChange(S.detail)}
        @vector-cancel=${()=>this._cancelPreview()}
        @vector-constraint-hit=${S=>this._handleConstraintHit(S.detail)}
      >
        <div class="toolbar">
          <div>
            <h2>Recipe Designer</h2>
            <p data-status>${this._statusText()}</p>
          </div>
          <div class="toolbar-actions">
            <button
              data-action="undo"
              ?disabled=${!this._history.length}
              @click=${()=>this._undo()}
            >
              Undo
            </button>
            <button
              data-action="redo"
              ?disabled=${!this._future.length}
              @click=${()=>this._redo()}
            >
              Redo
            </button>
            <button
              data-action="reset"
              @click=${()=>this._loadWorkspace(this._workspace||{})}
            >
              Reset
            </button>
          </div>
        </div>

        <div class="visual-grid">
          <article class="surface chart-card">
            <div class="card-heading">
              <h3>Quality Radar</h3>
              <span>axis reach</span>
            </div>
            <vector-radar-chart
              .axes=${ae.axes}
              .value=${ae.value}
              .previewValue=${ae.previewValue}
              .targetValue=${ae.targetValue}
              .reachableRange=${ae.reachableRange}
              .selectedKey=${ae.selectedKey}
              .hits=${ae.hits}
              .precision=${ae.precision}
              .showValues=${ae.showValues}
            ></vector-radar-chart>
            <div class="quality-rail-list">
              ${ue(this._qualityAxes,S=>S.key,S=>this._qualityRow(S,e,n))}
            </div>
          </article>

          <article class="surface chart-card">
            <div class="card-heading">
              <h3>Oil Blend</h3>
              <span
                >${w(Ln(this._preview?.oilVector||this._oilVector),1)}%</span
              >
            </div>
            <vector-pie-chart
              .items=${le.items}
              .value=${le.value}
              .constraints=${le.constraints}
              .total=${le.total}
              .selectedKey=${le.selectedKey}
              .hits=${le.hits}
              .precision=${le.precision}
              .centerValue=${le.centerValue}
              .centerLabel=${le.centerLabel}
              .centerPrecision=${le.centerPrecision}
              .palette=${le.palette}
            >
              ${Gt}
              <vector-constraint-rail
                slot="after-legend"
                data-oil-rail
                .item=${j.item}
                .value=${j.value}
                .constraint=${j.constraint}
                .role=${j.role}
                .hits=${j.hits}
                .rangeMin=${j.rangeMin}
                .rangeMax=${j.rangeMax}
                .precision=${j.precision}
                .palette=${j.palette}
                .variant=${j.variant}
              ></vector-constraint-rail>
            </vector-pie-chart>
          </article>

          <aside class="side-stack">
            <article class="surface">
              <div class="card-heading">
                <h3>Limits</h3>
                <span>${h.length}</span>
              </div>
              <constraint-feedback-list
                .hits=${h}
              ></constraint-feedback-list>
            </article>
          </aside>
        </div>

        <div class="control-grid">
          <article class="surface formula-panel">
            <div class="card-heading"><h3>Formula</h3></div>
            <div class="formula-stack">
              <div class="measurement-toggles system-only">
                <vector-unit-toggle
                  .label=${"System"}
                  .groupKey=${"system"}
                  .options=${ON}
                  .unitKey=${this._measurementUnitKey}
                  .measurementSystem=${this._measurementSystem}
                ></vector-unit-toggle>
              </div>
              <div class="batch-size-grid">
                <vector-quantity-input
                  .item=${c}
                  .value=${i.oil_weight_g||0}
                  .constraint=${this._formulaConstraints.oil_weight_g||{}}
                  .unitOptions=${c.unitOptions??[]}
                  .unitKey=${""}
                  .measurementSystem=${this._measurementSystem}
                  .basis=${"weight"}
                  .measurementMeasure=${"weight"}
                  .role=${"formula"}
                  .context=${a}
                  .palette=${this._palette}
                ></vector-quantity-input>
                <vector-quantity-input
                  .item=${l}
                  .value=${i.batch_volume_ml||0}
                  .constraint=${this._formulaConstraints.batch_volume_ml||{}}
                  .unitOptions=${l.unitOptions??[]}
                  .unitKey=${""}
                  .measurementSystem=${this._measurementSystem}
                  .basis=${"volume"}
                  .measurementMeasure=${"volume"}
                  .role=${"formula"}
                  .context=${a}
                  .palette=${this._palette}
                ></vector-quantity-input>
              </div>
              <vector-choice-control
                .item=${zN}
                .options=${qN}
                .value=${this._formulaChoices.lye_type}
                .role=${"formula_choice"}
              ></vector-choice-control>
              <vector-amount-list
                .items=${Wt}
                .values=${i}
                .constraints=${this._formulaConstraints}
                .thresholds=${E}
                .unitPreferences=${this._unitPreferences}
                .measurementSystem=${this._measurementSystem}
                .basis=${"percent_oils"}
                .measurementScope=${"oils"}
                .measurementMeasure=${"scalar"}
                .context=${a}
                .role=${"formula"}
                .control=${"slider"}
                .unitDisplay=${"text"}
                .activeEmphasis=${!1}
                .palette=${this._palette}
              ></vector-amount-list>
            </div>
          </article>

          <article class="surface additive-panel">
            <div class="card-heading"><h3>Additives</h3></div>
            <div class="additive-stack">
              <div class="measurement-toggles additive-toggles">
                <vector-unit-toggle
                  .label=${"Basis"}
                  .groupKey=${"scope"}
                  .options=${PN}
                  .unitKey=${this._measurementScopeKey}
                  .measurementScope=${this._measurementScope}
                ></vector-unit-toggle>
                <vector-unit-toggle
                  .label=${"Measure"}
                  .groupKey=${"measure"}
                  .options=${LN}
                  .unitKey=${this._measurementMeasureKey}
                  .measurementMeasure=${this._measurementMeasure}
                ></vector-unit-toggle>
              </div>
              <vector-amount-list
                .items=${u}
                .values=${s}
                .constraints=${this._additiveConstraints}
                .thresholds=${b}
                .unitPreferences=${this._unitPreferences}
                .measurementSystem=${this._measurementSystem}
                .basis=${V}
                .measurementScope=${this._measurementScope}
                .measurementMeasure=${this._measurementMeasure}
                .context=${a}
                .role=${"additive"}
                .control=${"slider"}
                .palette=${this._palette}
              ></vector-amount-list>
              <vector-summary-meter
                .items=${D}
              ></vector-summary-meter>
            </div>
          </article>

          <article class="surface">
            <div class="card-heading">
              <h3>Oil Library</h3>
              <span>${this._selectedOilItems().length} selected</span>
            </div>
            <oil-search-list
              .items=${this._oilItems}
              .selectedKeys=${Object.keys(this._oilVector)}
              .palette=${this._palette}
            ></oil-search-list>
          </article>
        </div>
      </section>
    `}_loadWorkspace(e){this._oilItems=kw(e),this._oilVector=Aw(e),this._oilConstraints=Ow(this._oilItems.map(n=>n.key)),this._qualityAxes=Rr,this._qualityConstraints=Object.fromEntries(this._qualityAxes.map(n=>[n.key,{min:n.min,max:n.max,preferredMin:n.preferredMin,preferredMax:n.preferredMax,step:n.step||1}])),this._qualityVector=Pt(this._oilVector),this._reachableRange=Un(this._oilVector,this._oilConstraints,this._qualityAxes),this._selectedOil=Object.keys(this._oilVector)[0]||"",this._selectedQuality=this._qualityAxes[0]?.key||"",this._selectionRole="composition",this._preview=null,this._hits=[],this._history=[],this._future=[],this._measurementSystem="metric",this._measurementUnitKey="metric",this._measurementScope="oils",this._measurementScopeKey="oils",this._measurementMeasure="weight",this._measurementMeasureKey="weight",this._measurementBasis="weight",this._batchOilWeightG=Number(e?.batches?.[0]?.oil_weight_g||1800),this._unitPreferences={},this._formulaValues=Object.fromEntries([...rt,...Wt].map(n=>[n.key,n.defaultValue??0])),this._formulaValues.oil_weight_g=this._batchOilWeightG,this._formulaChoices={lye_type:"naoh"},this._selectedAdditives=["fragrance","kaolin_clay","sodium_lactate"],this._additiveValues=this._initialAdditiveValues(),this._selectedAdditive="fragrance",this._scalarPreview=null,this._initialized=!0,this.requestUpdate()}_initialAdditiveValues(){let e=new Set(this._selectedAdditives||[]);return Object.fromEntries(this._additiveItems.map(n=>[n.key,e.has(n.key)?n.defaultValue??0:0]))}_handleSelect(e){let{source:n,role:r,key:o}=e;if(o){if(this._additiveItems.some(i=>i.key===o)||[...rt,...Wt].some(i=>i.key===o)){this._selectedAdditive=o,this.requestUpdate();return}r==="metric"||this._qualityAxes.some(i=>i.key===o)?(this._selectedQuality=o,this._selectionRole="metric"):n==="constraint"&&this._qualityAxes.some(i=>i.key===o)?(this._selectedQuality=o,this._selectionRole="metric"):(this._selectedOil=o,this._selectionRole="composition"),this.requestUpdate()}}_handlePreview(e){this._queuePreview(e)}_queuePreview(e){if(this._pendingPreviewDetail=e||{},this._previewFrame)return;let n=()=>{this._previewFrame=0;let o=this._pendingPreviewDetail;this._pendingPreviewDetail=null,o&&this._applyPreview(o)},r=globalThis.requestAnimationFrame;if(typeof r!="function"){n();return}this._previewFrame=r(n)}_flushPreview(){if(!this._pendingPreviewDetail)return;if(this._previewFrame){let n=globalThis.cancelAnimationFrame;typeof n=="function"&&n(this._previewFrame),this._previewFrame=0}let e=this._pendingPreviewDetail;this._pendingPreviewDetail=null,this._applyPreview(e)}_clearPendingPreview(){if(this._pendingPreviewDetail=null,!this._previewFrame)return;let e=globalThis.cancelAnimationFrame;typeof e=="function"&&e(this._previewFrame),this._previewFrame=0}_applyPreview(e){if(e.role==="additive"||e.role==="formula"){this._previewScalar(e);return}if(e.role==="constraint"){this._previewConstraint(e);return}if(e.role==="composition"){let n=this._solveCompositionDetail(e);this._preview={transactionId:e.transactionId,oilVector:n.oilVector,qualityVector:n.qualityVector,oilConstraints:this._oilConstraints,qualityConstraints:this._qualityConstraints,reachableRange:this._reachableRange,hits:n.hits},this.requestUpdate();return}if(e.role==="metric"){let n=this._metricTarget(e),r=Fo({oilVector:this._oilVector,targetQuality:n,oilConstraints:this._activeOilConstraints(),axes:this._qualityAxes});this._preview={transactionId:e.transactionId,oilVector:r.oilVector,qualityVector:r.qualityVector,oilConstraints:this._oilConstraints,qualityConstraints:this._qualityConstraints,reachableRange:this._reachableRange,targetQuality:n,hits:r.hits},this.requestUpdate()}}_handleChange(e){if(this._flushPreview(),e.role==="measurement"){this._changeMeasurement(e);return}if(e.role==="formula_choice"){this._formulaChoices=T(m({},this._formulaChoices),{[e.key]:String(e.value??"")}),this.requestUpdate();return}if(e.role==="selection"){if(e.listType==="ingredient"||this._additiveItems.some(n=>n.key===e.key)){this._changeAdditiveSelection(e.key,e.action);return}this._changeOilSelection(e.key,e.action);return}if(e.role==="additive"||e.role==="formula"){this._commitScalar(e);return}if(e.role==="constraint"){if(this._commitConstraintPreview(e))return;this._changeConstraint(e);return}if(e.role==="composition"){let n=this._solveCompositionDetail(e);this._commitOilVector(n.oilVector,n.hits);return}if(e.role==="metric"){let n=this._metricTarget(e),r=Fo({oilVector:this._oilVector,targetQuality:n,oilConstraints:this._activeOilConstraints(),axes:this._qualityAxes});this._commitOilVector(r.oilVector,r.hits)}}_handleConstraintHit(e){this._hits=e.hits||[],e.key&&(this._additiveItems.some(n=>n.key===e.key)||Wt.some(n=>n.key===e.key)?this._selectedAdditive=e.key:this._qualityAxes.some(n=>n.key===e.key)?(this._selectedQuality=e.key,this._selectionRole="metric"):(this._selectedOil=e.key,this._selectionRole="composition")),this.requestUpdate()}_previewScalar(e){let n=e.key,r=e.role,o=r==="formula"?this._formulaValues:this._additiveValues,i=this._scalarTarget(e,o[n]??0),s=r==="formula"?this._resolveFormulaValues(T(m({},o),{[n]:i}),n,i):T(m({},o),{[n]:i});this._scalarPreview={scope:r,key:n,values:s,hits:this._scalarHits(r==="formula"?s:this._formulaValues,r==="additive"?s:this._additiveValues)},this.requestUpdate()}_commitScalar(e){let n=e.key,r=e.role,o=this._scalarTarget(e,r==="formula"?this._formulaValues[n]:this._additiveValues[n]);if(r==="formula"){let i=this._resolveFormulaValues(T(m({},this._formulaValues),{[n]:o}),n,o);this._batchOilWeightG=Number(i.oil_weight_g||this._batchOilWeightG),this._formulaValues=i}else this._additiveValues=T(m({},this._additiveValues),{[n]:o}),this._selectedAdditives=this._activeAdditiveKeys(this._additiveValues);this._scalarPreview=null,this.requestUpdate()}_scalarTarget(e,n=0){let r=e.key;return Number(e.canonicalValue??e.proposedValue?.[r]??e.requestedValue?.[r]??n??0)}_changeMeasurement(e){if(e.key==="system"){this._measurementUnitKey=e.unitKey||this._measurementUnitKey,this._measurementSystem=e.measurementSystem||this._measurementSystem,this._unitPreferences={},this.requestUpdate();return}if(e.key==="scope"){this._measurementScopeKey=e.unitKey||this._measurementScopeKey,this._measurementScope=e.measurementScope||(e.unitKey==="oils"||e.unitKey==="batch"?e.unitKey:this._measurementScope),this._measurementBasis=this._activeMeasurementBasis(),this._unitPreferences={},this.requestUpdate();return}if(e.key==="measure"){this._measurementMeasureKey=e.unitKey||this._measurementMeasureKey,this._measurementMeasure=e.measurementMeasure||(e.unitKey==="weight"||e.unitKey==="volume"||e.unitKey==="scalar"?e.unitKey:this._measurementMeasure),this._measurementBasis=this._activeMeasurementBasis(),this._unitPreferences={},this.requestUpdate();return}if(e.key&&e.unitKey){this._unitPreferences=T(m({},this._unitPreferences),{[e.key]:e.unitKey}),this.requestUpdate();return}this._measurementUnitKey=e.unitKey||this._measurementUnitKey,this._measurementSystem=e.measurementSystem||this._measurementSystem,this._measurementBasis=e.basis||this._activeMeasurementBasis(),this.requestUpdate()}_changeAdditiveSelection(e,n){if(!e)return;let r=new Set(this._selectedAdditives),o=this._additiveItems.find(i=>i.key===e);n==="remove"?r.delete(e):(r.add(e),!(e in this._additiveValues)&&o&&(this._additiveValues=T(m({},this._additiveValues),{[e]:o.defaultValue??0}))),this._selectedAdditives=[...r],this._selectedAdditive=e,this._scalarPreview=null,this.requestUpdate()}_previewConstraint(e){let n=this._resolveConstraintPreview(e);this._preview=n,this.requestUpdate()}_commitConstraintPreview(e){return!this._preview||this._preview.transactionId!==e.transactionId||!this._preview.constraintRole?!1:(this._pushHistory(),this._oilVector=pe(this._preview.oilVector||this._oilVector),this._qualityVector=m({},this._preview.qualityVector),this._oilConstraints=this._preview.oilConstraints||this._oilConstraints,this._qualityConstraints=this._preview.qualityConstraints||this._qualityConstraints,this._reachableRange=this._preview.reachableRange||Un(this._oilVector,this._oilConstraints,this._qualityAxes),this._hits=this._preview.hits||[],this._preview=null,this._future=[],this.requestUpdate(),!0)}_resolveConstraintPreview(e){return e.constraintRole!=="composition"?this._resolveQualityConstraintPreview(e):this._resolveOilConstraintPreview(e)}_resolveOilConstraintPreview(e){let n=e.key,r=this._oilConstraints,o=this._oilVector,i=this._constraintPatchForLockedValue(n,e.constraint||{},r[n]||{},Number(o[n]||0),"%"),s=Vw(o,r,n,i.constraint,this._selectedOilItems()),a=Un(s.oilVector,s.oilConstraints,this._qualityAxes);return{transactionId:e.transactionId,constraintRole:"composition",oilVector:s.oilVector,oilConstraints:s.oilConstraints,qualityVector:s.qualityVector,qualityConstraints:this._qualityConstraints,reachableRange:a,hits:i.hits}}_resolveQualityConstraintPreview(e){let n=e.key,r=this._qualityConstraints,o=Number(this._qualityVector[n]||0),i=this._constraintPatchForLockedValue(n,e.constraint||{},r[n]||{},o,""),s=T(m({},r),{[n]:m(m({},r[n]||{}),i.constraint)}),a=s[n]||{},c=K(o,Number(a.min??-1/0),Number(a.max??1/0)),l=this._oilVector,u=this._qualityVector,d=i.hits;if(!d.length&&Number.isFinite(c)&&Math.abs(c-o)>.001){let h=Fo({oilVector:this._oilVector,targetQuality:T(m({},this._qualityVector),{[n]:c}),oilConstraints:this._oilConstraints,axes:this._qualityAxes});l=h.oilVector,u=h.qualityVector,d=h.hits||[]}return{transactionId:e.transactionId,constraintRole:"metric",oilVector:l,oilConstraints:this._oilConstraints,qualityVector:u,qualityConstraints:s,reachableRange:this._reachableRange,hits:d}}_constraintPatchForLockedValue(e,n,r,o,i){let a=!!m(m({},r),n).locked,c=m({},n),l=[];return a&&n.min!=null&&Number(n.min)>o&&(c.min=o,l.push(he(e,"locked",`${this._labelForKey(e)} is locked at ${w(o,1)}${i}.`))),a&&n.max!=null&&Number(n.max)<o&&(c.max=o,l.push(he(e,"locked",`${this._labelForKey(e)} is locked at ${w(o,1)}${i}.`))),{constraint:c,hits:l}}_solveCompositionDetail(e){if(e.proposedValue&&Object.keys(e.proposedValue).length>1){let o=pe(e.proposedValue);return{oilVector:o,qualityVector:Pt(o),hits:e.hits||[]}}let n=e.key,r=Number(e.proposedValue?.[n]??e.requestedValue?.[n]??this._oilVector[n]??0);return $w({oilVector:this._oilVector,key:n,target:r,oilConstraints:this._activeOilConstraints(),oilItems:this._selectedOilItems()})}_metricTarget(e){let n=e.key;return e.requestedValue&&Object.keys(e.requestedValue).length>1?e.requestedValue:T(m({},this._qualityVector),{[n]:Number(e.proposedValue?.[n]??e.requestedValue?.[n]??this._qualityVector[n]??0)})}_changeConstraint(e){this._preview=this._resolveConstraintPreview(e),this._commitConstraintPreview(e)}_changeOilSelection(e,n){if(e){if(this._pushHistory(),n==="add"&&!(e in this._oilVector))this._oilVector=T(m({},this._oilVector),{[e]:0}),this._selectedOil=e;else if(n==="remove"&&e in this._oilVector){if(Number(this._oilVector[e]||0)>0){let r=Mr({value:this._oilVector,key:e,target:0,constraints:this._oilConstraints,items:this._selectedOilItems(),total:100});this._oilVector=r.vector,this._hits=r.hits}delete this._oilVector[e],this._selectedOil=Object.keys(this._oilVector)[0]||""}this._qualityVector=Pt(this._oilVector),this._reachableRange=Un(this._oilVector,this._oilConstraints,this._qualityAxes),this._preview=null,this._future=[],this.requestUpdate()}}_commitOilVector(e,n=[]){if(ul(this._oilVector,e)<.001){this._preview=null,this._hits=n,this.requestUpdate();return}this._pushHistory(),this._oilVector=pe(e),this._qualityVector=Pt(this._oilVector),this._reachableRange=Un(this._oilVector,this._oilConstraints,this._qualityAxes),this._preview=null,this._hits=n,this._future=[],this.requestUpdate()}_pushHistory(){this._history=[...this._history,pe(this._oilVector)].slice(-50)}_undo(){let e=this._history.at(-1);e&&(this._future=[pe(this._oilVector),...this._future].slice(0,50),this._history=this._history.slice(0,-1),this._oilVector=pe(e),this._qualityVector=Pt(this._oilVector),this._reachableRange=Un(this._oilVector,this._oilConstraints,this._qualityAxes),this._preview=null,this._hits=[],this.requestUpdate())}_redo(){let e=this._future[0];e&&(this._history=[...this._history,pe(this._oilVector)].slice(-50),this._future=this._future.slice(1),this._oilVector=pe(e),this._qualityVector=Pt(this._oilVector),this._reachableRange=Un(this._oilVector,this._oilConstraints,this._qualityAxes),this._preview=null,this._hits=[],this.requestUpdate())}_cancelPreview(){this._clearPendingPreview(),this._preview=null,this.requestUpdate()}_emitQualityRange(e,n){let r=e.currentTarget,o=r.dataset.qualityRange,i=this._qualityAxes.find(d=>d.key===o);if(!i)return;let s=i.key;n==="vector-preview"&&!r.dataset.transaction&&(r.dataset.transaction=P("quality-row"));let a=r.dataset.transaction||P("quality-row"),c=Number(r.value);this._selectedQuality=s,this._selectionRole="metric";let l=T(m({},this._qualityVector),{[s]:c}),u={source:"table",role:"metric",transactionId:a,key:s,previousValue:this._qualityVector,requestedValue:l,proposedValue:l,delta:w(c-Number(this._qualityVector[s]||0),4),hits:[]};n==="vector-preview"?this._handlePreview(u):(this._handleChange(u),delete r.dataset.transaction)}_qualityRow(e,n,r){let o=r[e.key]||{},i=Number(n[e.key]||0),s=Number(o.min??e.reachableMin??e.min??0),a=Number(o.max??e.reachableMax??e.max??100),c=e.preferredMin??s,l=e.preferredMax??a,u=e.key===this._selectedQuality,d=Ce(K(i,e.min??0,e.max??100),e.step||1);return C`
      <div
        class=${G({"quality-row":!0,selected:u})}
        data-quality=${e.key}
        @click=${()=>this._selectQuality(e.key)}
      >
        <button class="quality-name" type="button" data-quality=${e.key}>
          ${e.label}
        </button>
        <div class="quality-combo">
          <div class="quality-track" aria-hidden="true">
            <span
              class="quality-reach"
              ${de({"--quality-start":`${this._qualityPct(s,e)}%`,"--quality-width":`${Math.max(0,this._qualityPct(a,e)-this._qualityPct(s,e))}%`})}
            ></span>
            <span
              class="quality-preferred"
              ${de({"--quality-start":`${this._qualityPct(c,e)}%`,"--quality-width":`${Math.max(0,this._qualityPct(l,e)-this._qualityPct(c,e))}%`})}
            ></span>
            <span
              class="quality-current"
              ${de({"--quality-position":`${this._qualityPct(i,e)}%`})}
            ></span>
          </div>
          <input
            data-quality-range=${e.key}
            type="range"
            min=${e.min??0}
            max=${e.max??100}
            step=${e.step||1}
            .value=${String(w(d,1))}
            aria-label=${`${e.label} target`}
            @input=${h=>this._emitQualityRange(h,"vector-preview")}
            @change=${h=>this._emitQualityRange(h,"vector-change")}
          />
        </div>
        <output>${w(i,1)}</output>
        <span class="quality-reach-label"
          >${w(s,1)}-${w(a,1)}</span
        >
      </div>
    `}_selectQuality(e){this._selectedQuality=e,this._selectionRole="metric",this.requestUpdate()}_qualityPct(e,n){let r=Number(n.min??0),o=Number(n.max??100);return K((Number(e||0)-r)/(o-r||1)*100,0,100)}_activeOilConstraints(){return this._preview?.oilConstraints||this._oilConstraints}_activeQualityConstraints(){return this._preview?.qualityConstraints||this._qualityConstraints}_activeReachableRange(){return this._preview?.reachableRange||this._reachableRange}_activeFormulaValues(){let e=this._scalarPreview?.scope==="formula"?this._scalarPreview.values:this._formulaValues;return this._withBatchFormulaValues(e)}_activeAdditiveValues(){return this._scalarPreview?.scope==="additive"?this._scalarPreview.values:this._additiveValues}_activeAdditiveKeys(e=this._activeAdditiveValues()){return this._additiveItems.map(n=>n.key).filter(n=>Math.abs(Number(e?.[n]||0))>1e-9)}_withBatchFormulaValues(e=this._formulaValues){let n=Number(e.oil_weight_g||this._batchOilWeightG||0),r=this._batchEstimateForOilWeight(n,e,this._activeAdditiveValues(),this._preview?.oilVector||this._oilVector);return T(m({},e),{oil_weight_g:w(n,1),batch_volume_ml:w(r.volumeMl,1)})}_resolveFormulaValues(e,n,r){let o=m({},e);if(n==="batch_volume_ml"){let i=this._oilWeightForBatchVolume(r,o,this._activeAdditiveValues(),this._preview?.oilVector||this._oilVector);o.oil_weight_g=w(i,1)}else n==="oil_weight_g"&&(o.oil_weight_g=w(r,1));return this._withBatchFormulaValues(o)}_activeMeasurementBasis(){return this._measurementMeasure==="volume"?"volume":"weight"}_batchContext(e=this._oilVector,n=this._activeFormulaValues(),r=this._activeAdditiveValues()){let o=this._batchEstimateForOilWeight(Number(n.oil_weight_g||this._batchOilWeightG||0),n,r,e);return T(m({},o),{densities:this._workspace?.constants?.density||{}})}_batchItems(e){return rt.map(n=>n.key!=="batch_volume_ml"?n:T(m({},n),{defaultValue:w(Number(e.volumeMl??0),1)}))}_batchSizeItem(){return rt.find(e=>e.key===(this._measurementMeasure==="volume"?"batch_volume_ml":"oil_weight_g"))||rt[0]}_batchSizeUnitOptions(){let e=this._measurementMeasure==="volume"?"volume":"weight";return(this._batchSizeItem().unitOptions??[]).filter(n=>n.measure===e)}_additiveItemsForContext(e){let n=e?.densities||{};return this._additiveItems.map(r=>{let o=r.material||r.key,i=Number.isFinite(Number(n[o]));return T(m({},r),{unitOptions:(r.unitOptions||[]).filter(s=>s.measure!=="volume"||i)})})}_batchEstimateForOilWeight(e,n=this._formulaValues,r=this._additiveValues,o=this._oilVector){return Ew({oilWeightG:e,oilVector:o,formulaValues:n,additiveValues:r,selectedAdditives:this._activeAdditiveKeys(r),additiveItems:this._additiveItems,workspace:this._workspace})}_oilWeightForBatchVolume(e,n=this._formulaValues,r=this._additiveValues,o=this._oilVector){let s=this._batchEstimateForOilWeight(1e3,n,r,o).volumeMl/1e3||1;return K(Number(e||0)/s,rt[0].min,rt[0].max)}_selectedOilItems(){let e=new Set(Object.keys(this._oilVector));return this._oilItems.filter(n=>e.has(n.key))}_labelForKey(e){return this._oilItems.find(n=>n.key===e)?.label||this._qualityAxes.find(n=>n.key===e)?.label||[...rt,...Wt,...this._additiveItems].find(n=>n.key===e)?.label||e}_activeHits(){return this._preview?.hits||this._hits||[]}_scalarHits(e=this._formulaValues,n=this._additiveValues){let r=Wt.flatMap(s=>this._thresholdHit(s,e[s.key])),o=new Set(this._activeAdditiveKeys(n)),i=this._additiveItems.filter(s=>o.has(s.key)).flatMap(s=>this._thresholdHit(s,n[s.key]));return[...r,...i]}_thresholdHit(e,n){let r=(e.thresholds||[]).find(o=>Number(n||0)>=Number(o.min)&&Number(n||0)<=Number(o.max));return!r||r.severity==="ok"?[]:[he(e.key,"threshold",r.label||`${e.label||e.key} is in ${r.severity} range.`,r.severity==="danger"?"hard":"soft")]}_formulaSummary(e,n){let r=new Set(this._activeAdditiveKeys(n)),o=Object.entries(n||{}).filter(([a])=>r.has(a)).reduce((a,[,c])=>a+Number(c||0),0),i=Number(n.fragrance||0),s=Number(e.lye_concentration||0);return[{key:"batch_volume_ml",label:"Batch volume",value:Number(e.batch_volume_ml||0),min:400,max:16e3,unit:" ml",precision:0,thresholds:[]},{key:"total_additives",label:"Additive load",value:o,min:0,max:10,unit:"%",precision:1,thresholds:[{key:"normal",min:0,max:5,severity:"ok"},{key:"busy",min:5.1,max:8,severity:"warn"},{key:"crowded",min:8.1,max:10,severity:"danger"}]},{key:"fragrance_load",label:"Fragrance load",value:i,min:0,max:6,unit:"%",precision:1,thresholds:this._additiveItems.find(a=>a.key==="fragrance")?.thresholds||[]},{key:"lye_strength",label:"Lye strength",value:s,min:25,max:45,unit:"%",precision:1,thresholds:Wt.find(a=>a.key==="lye_concentration")?.thresholds||[]}]}_statusText(){return this._preview?"Previewing a recipe change.":this._hits.length?this._hits[0].message:"Approximate quality model; fatty-acid data is a demo fixture."}};WN()});var Dl,Bw=f(()=>{"use strict";fe();sp();Hw();fe();Dl=class t{constructor(){this.workspace=y(So)}static{this.\u0275fac=function(n){return new(n||t)}}static{this.\u0275cmp=pr({type:t,selectors:[["rms-design-page"]],decls:2,vars:1,consts:[["aria-label","Recipe designer"],[3,"workspace"]],template:function(n,r){n&1&&(Ri(0,"section",0),ac(1,"recipe-vector-workbench",1),ki()),n&2&&(co(),cc("workspace",r.workspace.workspace()))},encapsulation:2})}}});var zw,qw=f(()=>{"use strict";Bw();zw=[{path:"",pathMatch:"full",component:Dl},{path:"**",redirectTo:""}]});var Ww,Gw=f(()=>{"use strict";fe();ip();Qb();qw();Ww={providers:[_d(),op(zw),Kb("ngsw-worker.js",{enabled:!Sy(),registrationStrategy:"registerWhenStable:30000"})]}});var GN=Xw(()=>{Nf();qb();Gw();Mf(Wc,Ww).catch(t=>console.error(t))});export default GN();
