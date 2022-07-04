//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! Subscribing primitives.
use crate::prelude::{KeyExpr, Sample};
use crate::subscriber::{SubscriberInvoker, SubscriberState};
use crate::sync::channel::Receiver;
use crate::sync::ZFuture;
use crate::time::Period;
use crate::API_DATA_RECEPTION_CHANNEL_SIZE;
use crate::{Result as ZResult, SessionRef};
use async_std::sync::Arc;
use flume::r#async::RecvFut;
use flume::{bounded, Iter, RecvError, RecvTimeoutError, TryIter, TryRecvError};
use std::fmt;
use std::pin::Pin;
use std::sync::RwLock;
use std::task::{Context, Poll};
use zenoh_protocol_core::SubInfo;
use zenoh_sync::{derive_zfuture, zreceiver, Runnable};

/// The subscription mode.
pub use zenoh_protocol_core::SubMode;

/// The kind of reliability.
pub use zenoh_protocol_core::Reliability;

/// The callback that will be called on each data for a [`CallbackSharedSubscriber`](CallbackSharedSubscriber).
pub type DataHandler = dyn FnMut(Sample) + Send + Sync + 'static;

zreceiver! {
    /// A [`Receiver`] of [`Sample`](crate::prelude::Sample) returned by
    /// [`SharedSubscriber::receiver()`](SharedSubscriber::receiver).
    ///
    /// `SampleReceiver` implements the `Stream` trait as well as the
    /// [`Receiver`](crate::prelude::Receiver) trait which allows to access the samples:
    ///  - synchronously as with a [`std::sync::mpsc::Receiver`](std::sync::mpsc::Receiver)
    ///  - asynchronously as with a [`async_std::channel::Receiver`](async_std::channel::Receiver).
    /// `SampleReceiver` also provides a [`recv_async()`](SampleReceiver::recv_async) function which allows
    /// to access samples asynchronously without needing a mutable reference to the `SampleReceiver`.
    ///
    /// `SampleReceiver` implements `Clonable` and it's lifetime is not bound to it's associated
    /// [`SharedSubscriber`]. This is useful to move multiple instances to multiple threads/tasks and perform
    /// job stealing. When the associated [`SharedSubscriber`] is closed or dropped and all samples
    /// have been received [`Receiver::recv()`](Receiver::recv) will return  `Error` and
    /// `Receiver::next()` will return `None`.
    ///
    /// Examples:
    /// ```
    /// # async_std::task::block_on(async {
    /// use futures::prelude::*;
    /// use zenoh::prelude::*;
    /// let session = zenoh::open(config::peer()).await.unwrap();
    ///
    /// let mut SharedSubscriber = session.subscribe("/key/expression").await.unwrap();
    /// let task1 = async_std::task::spawn({
    ///     let mut receiver = SharedSubscriber.receiver().clone();
    ///     async move {
    ///         while let Some(sample) = receiver.next().await {
    ///             println!(">> Task1 received sample '{}'", sample);
    ///         }
    ///     }
    /// });
    /// let task2 = async_std::task::spawn({
    ///     let mut receiver = SharedSubscriber.receiver().clone();
    ///     async move {
    ///         while let Some(sample) = receiver.next().await {
    ///             println!(">> Task2 received sample '{}'", sample);
    ///         }
    ///     }
    /// });
    ///
    /// async_std::task::sleep(std::time::Duration::from_secs(1)).await;
    /// SharedSubscriber.close().await.unwrap();
    /// futures::join!(task1, task2);
    /// # })
    /// ```
    #[derive(Clone)]
    pub struct SampleReceiver : Receiver<Sample> {}
}

zreceiver! {
    /// A SharedSubscriber that provides data through a stream.
    ///
    /// `SharedSubscriber` implements the `Stream` trait as well as the
    /// [`Receiver`](crate::prelude::Receiver) trait which allows to access the samples:
    ///  - synchronously as with a [`std::sync::mpsc::Receiver`](std::sync::mpsc::Receiver)
    ///  - asynchronously as with a [`async_std::channel::Receiver`](async_std::channel::Receiver).
    /// `SharedSubscriber` also provides a [`recv_async()`](SharedSubscriber::recv_async) function which allows
    /// to access samples asynchronously without needing a mutable reference to the `SharedSubscriber`.
    ///
    /// SharedSubscribers are automatically undeclared when dropped.
    ///
    /// # Examples
    ///
    /// ### sync
    /// ```no_run
    /// # use zenoh::prelude::*;
    /// # let session = zenoh::open(config::peer()).wait().unwrap();
    ///
    /// let mut SharedSubscriber = session.subscribe("/key/expression").wait().unwrap();
    /// while let Ok(sample) = SharedSubscriber.recv() {
    ///      println!(">> Received sample '{}'", sample);
    /// }
    /// ```
    ///
    /// ### async
    /// ```no_run
    /// # async_std::task::block_on(async {
    /// # use futures::prelude::*;
    /// # use zenoh::prelude::*;
    /// # let session = zenoh::open(config::peer()).await.unwrap();
    ///
    /// let mut SharedSubscriber = session.subscribe("/key/expression").await.unwrap();
    /// while let Some(sample) = SharedSubscriber.next().await {
    ///      println!(">> Received sample '{}'", sample);
    /// }
    /// # })
    /// ```
    pub struct SharedSubscriber<'a> : Receiver<Sample> {
        pub(crate) session: SessionRef<'a>,
        pub(crate) state: Arc<SubscriberState>,
        pub(crate) alive: bool,
        pub(crate) sample_receiver: SampleReceiver,
    }
}

impl SharedSubscriber<'_> {
    /// Returns a `Clonable` [`SampleReceiver`] which lifetime is not bound to
    /// the associated `SharedSubscriber` lifetime.
    pub fn receiver(&mut self) -> &mut SampleReceiver {
        &mut self.sample_receiver
    }

    /// Pull available data for a pull-mode [`SharedSubscriber`](SharedSubscriber).
    ///
    /// # Examples
    /// ```
    /// # async_std::task::block_on(async {
    /// use futures::prelude::*;
    /// use zenoh::prelude::*;
    /// use zenoh::SharedSubscriber::SubMode;
    ///
    /// let session = zenoh::open(config::peer()).await.unwrap();
    /// let mut SharedSubscriber = session.subscribe("/key/expression")
    ///                             .mode(SubMode::Pull).await.unwrap();
    /// async_std::task::spawn(SharedSubscriber.receiver().clone().for_each(
    ///     move |sample| async move { println!("Received : {:?}", sample); }
    /// ));
    /// SharedSubscriber.pull();
    /// # })
    /// ```
    #[must_use = "ZFutures do nothing unless you `.wait()`, `.await` or poll them"]
    pub fn pull(&self) -> impl ZFuture<Output = ZResult<()>> {
        self.session.pull(&self.state.key_expr)
    }

    /// Close a [`SharedSubscriber`](SharedSubscriber) previously created with [`subscribe`](crate::Session::subscribe).
    ///
    /// SharedSubscribers are automatically closed when dropped, but you may want to use this function to handle errors or
    /// close the SharedSubscriber asynchronously.
    ///
    /// # Examples
    /// ```
    /// # async_std::task::block_on(async {
    /// use zenoh::prelude::*;
    ///
    /// let session = zenoh::open(config::peer()).await.unwrap();
    /// let SharedSubscriber = session.subscribe("/key/expression").await.unwrap();
    /// SharedSubscriber.close().await.unwrap();
    /// # })
    /// ```
    #[inline]
    #[must_use = "ZFutures do nothing unless you `.wait()`, `.await` or poll them"]
    pub fn close(mut self) -> impl ZFuture<Output = ZResult<()>> {
        self.alive = false;
        self.session.unsubscribe(self.state.id)
    }
}

impl Drop for SharedSubscriber<'_> {
    fn drop(&mut self) {
        if self.alive {
            let _ = self.session.unsubscribe(self.state.id).wait();
        }
    }
}

impl fmt::Debug for SharedSubscriber<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.state.fmt(f)
    }
}

/// A SharedSubscriber that provides data through a callback.
///
/// SharedSubscribers are automatically undeclared when dropped.
pub struct CallbackSharedSubscriber<'a> {
    pub(crate) session: SessionRef<'a>,
    pub(crate) state: Arc<SubscriberState>,
    pub(crate) alive: bool,
}

impl CallbackSharedSubscriber<'_> {
    /// Pull available data for a pull-mode [`CallbackSharedSubscriber`](CallbackSharedSubscriber).
    ///
    /// # Examples
    /// ```
    /// # async_std::task::block_on(async {
    /// use zenoh::prelude::*;
    /// use zenoh::SharedSubscriber::SubMode;
    ///
    /// let session = zenoh::open(config::peer()).await.unwrap();
    /// let SharedSubscriber = session.subscribe("/key/expression")
    ///     .callback(|sample| { println!("Received : {} {}", sample.key_expr, sample.value); })
    ///     .mode(SubMode::Pull).await.unwrap();
    /// SharedSubscriber.pull();
    /// # })
    /// ```
    #[must_use = "ZFutures do nothing unless you `.wait()`, `.await` or poll them"]
    pub fn pull(&self) -> impl ZFuture<Output = ZResult<()>> {
        self.session.pull(&self.state.key_expr)
    }

    /// Undeclare a [`CallbackSharedSubscriber`](CallbackSharedSubscriber).
    ///
    /// `CallbackSharedSubscribers` are automatically undeclared when dropped, but you may want to use this function to handle errors or
    /// undeclare the `CallbackSharedSubscriber` asynchronously.
    ///
    /// # Examples
    /// ```
    /// # async_std::task::block_on(async {
    /// use zenoh::prelude::*;
    ///
    /// let session = zenoh::open(config::peer()).await.unwrap();
    /// # fn data_handler(_sample: Sample) { };
    /// let SharedSubscriber = session.subscribe("/key/expression")
    ///     .callback(data_handler).await.unwrap();
    /// SharedSubscriber.undeclare().await.unwrap();
    /// # })
    /// ```
    #[inline]
    #[must_use = "ZFutures do nothing unless you `.wait()`, `.await` or poll them"]
    pub fn undeclare(mut self) -> impl ZFuture<Output = ZResult<()>> {
        self.alive = false;
        self.session.unsubscribe(self.state.id)
    }
}

impl Drop for CallbackSharedSubscriber<'_> {
    fn drop(&mut self) {
        if self.alive {
            let _ = self.session.unsubscribe(self.state.id).wait();
        }
    }
}

impl fmt::Debug for CallbackSharedSubscriber<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.state.fmt(f)
    }
}

derive_zfuture! {
    /// A builder for initializing a [`SharedSubscriber`](SharedSubscriber).
    ///
    /// The result of this builder can be accessed synchronously via [`wait()`](ZFuture::wait())
    /// or asynchronously via `.await`.
    ///
    /// # Examples
    /// ```
    /// # async_std::task::block_on(async {
    /// use zenoh::prelude::*;
    ///
    /// let session = zenoh::open(config::peer()).await.unwrap();
    /// let SharedSubscriber = session
    ///     .subscribe("/key/expression")
    ///     .best_effort()
    ///     .pull_mode()
    ///     .await
    ///     .unwrap();
    /// # })
    /// ```
    #[derive(Debug, Clone)]
    pub struct SharedSubscriberBuilder<'a, 'b> {
        pub(crate) session: SessionRef<'a>,
        pub(crate) key_expr: KeyExpr<'b>,
        pub(crate) reliability: Reliability,
        pub(crate) mode: SubMode,
        pub(crate) period: Option<Period>,
        pub(crate) local: bool,
    }
}

impl<'a, 'b> SharedSubscriberBuilder<'a, 'b> {
    /// Make the built SharedSubscriber a [`CallbackSharedSubscriber`](CallbackSharedSubscriber).
    #[inline]
    pub fn callback<DataHandler>(
        self,
        handler: DataHandler,
    ) -> CallbackSharedSubscriberBuilder<'a, 'b>
    where
        DataHandler: FnMut(Sample) + Send + Sync + 'static,
    {
        CallbackSharedSubscriberBuilder {
            session: self.session,
            key_expr: self.key_expr,
            reliability: self.reliability,
            mode: self.mode,
            period: self.period,
            local: self.local,
            handler: Arc::new(RwLock::new(handler)),
        }
    }

    /// Change the subscription reliability.
    #[inline]
    pub fn reliability(mut self, reliability: Reliability) -> Self {
        self.reliability = reliability;
        self
    }

    /// Change the subscription reliability to `Reliable`.
    #[inline]
    pub fn reliable(mut self) -> Self {
        self.reliability = Reliability::Reliable;
        self
    }

    /// Change the subscription reliability to `BestEffort`.
    #[inline]
    pub fn best_effort(mut self) -> Self {
        self.reliability = Reliability::BestEffort;
        self
    }

    /// Change the subscription mode.
    #[inline]
    pub fn mode(mut self, mode: SubMode) -> Self {
        self.mode = mode;
        self
    }

    /// Change the subscription mode to Push.
    #[inline]
    pub fn push_mode(mut self) -> Self {
        self.mode = SubMode::Push;
        self.period = None;
        self
    }

    /// Change the subscription mode to Pull.
    #[inline]
    pub fn pull_mode(mut self) -> Self {
        self.mode = SubMode::Pull;
        self
    }

    /// Change the subscription period.
    #[inline]
    pub fn period(mut self, period: Option<Period>) -> Self {
        self.period = period;
        self
    }

    /// Make the subscription local only.
    #[inline]
    pub fn local(mut self) -> Self {
        self.local = true;
        self
    }
}

impl<'a> Runnable for SharedSubscriberBuilder<'a, '_> {
    type Output = ZResult<SharedSubscriber<'a>>;

    fn run(&mut self) -> Self::Output {
        log::trace!("subscribe({:?})", self.key_expr);
        let (sender, receiver) = bounded(*API_DATA_RECEPTION_CHANNEL_SIZE);

        if self.local {
            self.session
                .declare_any_local_subscriber(
                    &self.key_expr,
                    SubscriberInvoker::Sender(sender),
                    true,
                )
                .map(|sub_state| {
                    SharedSubscriber::new(
                        self.session.clone(),
                        sub_state,
                        true,
                        SampleReceiver::new(receiver.clone()),
                        receiver,
                    )
                })
        } else {
            self.session
                .declare_any_subscriber(
                    &self.key_expr,
                    SubscriberInvoker::Sender(sender),
                    &SubInfo {
                        reliability: self.reliability,
                        mode: self.mode,
                        period: self.period,
                        shared: true,
                    },
                )
                .map(|sub_state| {
                    SharedSubscriber::new(
                        self.session.clone(),
                        sub_state,
                        true,
                        SampleReceiver::new(receiver.clone()),
                        receiver,
                    )
                })
        }
    }
}

derive_zfuture! {
    /// A builder for initializing a [`CallbackSharedSubscriber`](CallbackSharedSubscriber).
    ///
    /// The result of this builder can be accessed synchronously via [`wait()`](ZFuture::wait())
    /// or asynchronously via `.await`.
    ///
    /// # Examples
    /// ```
    /// # async_std::task::block_on(async {
    /// use zenoh::prelude::*;
    ///
    /// let session = zenoh::open(config::peer()).await.unwrap();
    /// let SharedSubscriber = session
    ///     .subscribe("/key/expression")
    ///     .callback(|sample| { println!("Received : {} {}", sample.key_expr, sample.value); })
    ///     .best_effort()
    ///     .pull_mode()
    ///     .await
    ///     .unwrap();
    /// # })
    /// ```
    #[derive(Clone)]
    pub struct CallbackSharedSubscriberBuilder<'a, 'b> {
        session: SessionRef<'a>,
        key_expr: KeyExpr<'b>,
        reliability: Reliability,
        mode: SubMode,
        period: Option<Period>,
        local: bool,
        handler: Arc<RwLock<DataHandler>>,
    }
}

impl fmt::Debug for CallbackSharedSubscriberBuilder<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallbackSharedSubscriberBuilder")
            .field("session", &self.session)
            .field("key_expr", &self.key_expr)
            .field("reliability", &self.reliability)
            .field("mode", &self.mode)
            .field("period", &self.period)
            .finish()
    }
}

impl<'a, 'b> CallbackSharedSubscriberBuilder<'a, 'b> {
    /// Change the subscription reliability.
    #[inline]
    pub fn reliability(mut self, reliability: Reliability) -> Self {
        self.reliability = reliability;
        self
    }

    /// Change the subscription reliability to `Reliable`.
    #[inline]
    pub fn reliable(mut self) -> Self {
        self.reliability = Reliability::Reliable;
        self
    }

    /// Change the subscription reliability to `BestEffort`.
    #[inline]
    pub fn best_effort(mut self) -> Self {
        self.reliability = Reliability::BestEffort;
        self
    }

    /// Change the subscription mode.
    #[inline]
    pub fn mode(mut self, mode: SubMode) -> Self {
        self.mode = mode;
        self
    }

    /// Change the subscription mode to Push.
    #[inline]
    pub fn push_mode(mut self) -> Self {
        self.mode = SubMode::Push;
        self.period = None;
        self
    }

    /// Change the subscription mode to Pull.
    #[inline]
    pub fn pull_mode(mut self) -> Self {
        self.mode = SubMode::Pull;
        self
    }

    /// Change the subscription period.
    #[inline]
    pub fn period(mut self, period: Option<Period>) -> Self {
        self.period = period;
        self
    }

    /// Make the subscription local onlyu.
    #[inline]
    pub fn local(mut self) -> Self {
        self.local = true;
        self
    }
}

impl<'a> Runnable for CallbackSharedSubscriberBuilder<'a, '_> {
    type Output = ZResult<CallbackSharedSubscriber<'a>>;

    fn run(&mut self) -> Self::Output {
        log::trace!("declare_callback_SharedSubscriber({:?})", self.key_expr);

        if self.local {
            self.session
                .declare_any_local_subscriber(
                    &self.key_expr,
                    SubscriberInvoker::Handler(self.handler.clone()),
                    false,
                )
                .map(|sub_state| CallbackSharedSubscriber {
                    session: self.session.clone(),
                    state: sub_state,
                    alive: true,
                })
        } else {
            self.session
                .declare_any_subscriber(
                    &self.key_expr,
                    SubscriberInvoker::Handler(self.handler.clone()),
                    &SubInfo {
                        reliability: self.reliability,
                        mode: self.mode,
                        period: self.period,
                        shared: false,
                    },
                )
                .map(|sub_state| CallbackSharedSubscriber {
                    session: self.session.clone(),
                    state: sub_state,
                    alive: true,
                })
        }
    }
}
