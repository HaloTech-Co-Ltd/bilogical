/*-------------------------------------------------------------------------
 *
 * bilogical.c
 *	  logical decoding plugin that help to prevent replicating the same 
 *    changes back and forth, based on pgoutput.
 *
 * 
 * 版权所有 (c) 2019-2026, 易景科技保留所有权利。
 * Copyright (c) 2019-2026, Halo Tech Co.,Ltd. All rights reserved.
 * 
 * 易景科技是Halo Database、Halo Database Management System、羲和数据
 * 库、羲和数据库管理系统（后面简称 Halo ）软件的发明人同时也为知识产权权
 * 利人。Halo 软件的知识产权，以及与本软件相关的所有信息内容（包括但不限
 * 于文字、图片、音频、视频、图表、界面设计、版面框架、有关数据或电子文档等）
 * 均受中华人民共和国法律法规和相应的国际条约保护，易景科技享有上述知识产
 * 权，但相关权利人依照法律规定应享有的权利除外。未免疑义，本条所指的“知识
 * 产权”是指任何及所有基于 Halo 软件产生的：（a）版权、商标、商号、域名、与
 * 商标和商号相关的商誉、设计和专利；与创新、技术诀窍、商业秘密、保密技术、非
 * 技术信息相关的权利；（b）人身权、掩模作品权、署名权和发表权；以及（c）在
 * 本协议生效之前已存在或此后出现在世界任何地方的其他工业产权、专有权、与“知
 * 识产权”相关的权利，以及上述权利的所有续期和延长，无论此类权利是否已在相
 * 关法域内的相关机构注册。
 *
 * This software and related documentation are provided under a license
 * agreement containing restrictions on use and disclosure and are 
 * protected by intellectual property laws. Except as expressly permitted
 * in your license agreement or allowed by law, you may not use, copy, 
 * reproduce, translate, broadcast, modify, license, transmit, distribute,
 * exhibit, perform, publish, or display any part, in any form, or by any
 * means. Reverse engineering, disassembly, or decompilation of this 
 * software, unless required by law for interoperability, is prohibited.
 * 
 * This software is developed for general use in a variety of
 * information management applications. It is not developed or intended
 * for use in any inherently dangerous applications, including applications
 * that may create a risk of personal injury. If you use this software or
 * in dangerous applications, then you shall be responsible to take all
 * appropriate fail-safe, backup, redundancy, and other measures to ensure
 * its safe use. Halo Corporation and its affiliates disclaim any 
 * liability for any damages caused by use of this software in dangerous
 * applications.
 * 
 *
 * IDENTIFICATION
 *	  contrib/bilogical/bilogical.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tupconvert.h"
#include "catalog/partition.h"
#include "catalog/pg_publication.h"
#if PG_VERSION_NUM >= 150000
#include "catalog/pg_publication_rel.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#endif
#include "commands/defrem.h"
#include "fmgr.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/origin.h"
#if PG_VERSION_NUM < 150000
#include "utils/int8.h"
#endif
#include "utils/inval.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

/* These must be available to dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);


#define BILOGICAL_ORIGIN_NONE "none"
#define BILOGICAL_ORIGIN_ANY  "any"

#define LOGICALREP_PROTO_VERSION_NUM13	1	/* server version 13 and below */
#define LOGICALREP_PROTO_VERSION_NUM14	2	/* server version 14 and above */
#define LOGICALREP_PROTO_VERSION_NUM15	3	/* server version 15 and above */
#define LOGICALREP_PROTO_VERSION_NUM16	4	/* server version 16 and above */

static bool bilogical_twophase = false;
static char	*bilogical_origin = BILOGICAL_ORIGIN_NONE;


typedef struct BilogicalData
{
	MemoryContext context;

	uint32		protocol_version;
	List	   *publication_names;
	List	   *publications;
	bool		binary;
	bool		streaming;
	bool		messages;
	bool		two_phase;
	char	   *origin;
} BilogicalData;

typedef struct BilogicalEState
{
	MemoryContext	ctxPub;
	MemoryContext	ctxCache;

	bool			is_pub_valid;
	bool			is_in_streaming;

	HTAB 			*relcache;
} BilogicalEState;

static BilogicalEState	BillEState = {
	NULL,
	NULL,
	false,
	false,
	NULL
};

#if PG_VERSION_NUM >= 150000
typedef struct BilogicalTxnData
{
	bool		sent_begin_txn;
} BilogicalTxnData;

enum RowFilterPubAction
{
	PUBACTION_INSERT,
	PUBACTION_UPDATE,
	PUBACTION_DELETE
};

#define NUM_ROWFILTER_PUBACTIONS (PUBACTION_DELETE + 1)

typedef struct RelationSyncEntry
{
	Oid			relid;

	bool		replicate_valid;

	bool		schema_sent;
	List	   *streamed_txns;

	PublicationActions pubactions;

	ExprState  *exprstate[NUM_ROWFILTER_PUBACTIONS];
	EState	   *estate;
	TupleTableSlot *new_slot;
	TupleTableSlot *old_slot;

	Oid			publish_as_relid;

	AttrMap    *attrmap;

	Bitmapset  *columns;

	MemoryContext entry_cxt;
} RelationSyncEntry;
#else
typedef struct RelationSyncEntry
{
	Oid			relid;

	bool		schema_sent;
	List	   *streamed_txns;

	bool		replicate_valid;
	PublicationActions pubactions;

	Oid			publish_as_relid;

	TupleConversionMap *map;
} RelationSyncEntry;
#endif

static void bill_startup(LogicalDecodingContext *ctx,
						 OutputPluginOptions *options,
						 bool is_init);
static void bill_begin(LogicalDecodingContext *ctx, 
					   ReorderBufferTXN *txn);
static void bill_change(LogicalDecodingContext *ctx, 
						ReorderBufferTXN *txn,
						Relation rel, 
						ReorderBufferChange *change);
static void bill_truncate(LogicalDecodingContext *ctx,
						  ReorderBufferTXN *txn, 
						  int nrelations, 
						  Relation relations[],
						  ReorderBufferChange *change);
static void bill_commit(LogicalDecodingContext *ctx, 
						ReorderBufferTXN *txn, 
						XLogRecPtr commit_lsn);
static void bill_message(LogicalDecodingContext *ctx,
						 ReorderBufferTXN *txn, 
						 XLogRecPtr message_lsn,
						 bool transactional, 
						 const char *prefix,
						 Size message_size, 
						 const char *message);
static bool bill_filter_by_origin(LogicalDecodingContext *ctx,
							   	  RepOriginId origin_id);
static void bill_shutdown(LogicalDecodingContext *ctx);


static void bill_stream_start(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn);
static void bill_stream_stop(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn);
static void bill_stream_abort(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn,
							  XLogRecPtr abort_lsn);
static void bill_stream_commit(LogicalDecodingContext *ctx,
							   ReorderBufferTXN *txn,
							   XLogRecPtr commit_lsn);
static void bill_stream_change(LogicalDecodingContext *ctx,
							   ReorderBufferTXN *txn,
							   Relation relation,
							   ReorderBufferChange *change);
static void bill_stream_message(LogicalDecodingContext *ctx,
								ReorderBufferTXN *txn,
								XLogRecPtr message_lsn,
								bool transactional,
								const char *prefix,
								Size message_size,
								const char *message);
static void bill_stream_truncate(LogicalDecodingContext *ctx,
								 ReorderBufferTXN *txn,
								 int nrelations,
								 Relation relations[],
								 ReorderBufferChange *change);

/*
 * TODO: support prepared transaction for 14 and below
 */ 
/* static bool bill_filter_prepare(LogicalDecodingContext *ctx,
 *								TransactionId xid,
 *								const char *gid);
 */
static void bill_begin_prepare(LogicalDecodingContext *ctx,
 							   ReorderBufferTXN *txn);
static void bill_prepare(LogicalDecodingContext *ctx,
						 ReorderBufferTXN *txn,
						 XLogRecPtr prepare_lsn);
static void bill_commit_prepared(LogicalDecodingContext *ctx,
							     ReorderBufferTXN *txn,
							     XLogRecPtr commit_lsn);
static void bill_rollback_prepared(LogicalDecodingContext *ctx,
								   ReorderBufferTXN *txn,
								   XLogRecPtr prepare_end_lsn,
								   TimestampTz prepare_time);
static void bill_stream_prepare(LogicalDecodingContext *ctx,
							    ReorderBufferTXN *txn,
							    XLogRecPtr prepare_lsn);


static void parse_plugin_params(List *options, BilogicalData *data);
static void bill_ctx_reset(void *arg);
static void pub_invalidation_cb(Datum arg, int cacheid, uint32 hashvalue);
static void init_relcache(MemoryContext ctxCache);
static void rel_sync_cache_relation_cb(Datum arg, Oid relid);
static bool get_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid);
static void cleanup_rel_sync_cache(TransactionId xid, bool is_commit);
static void set_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid);
static bool get_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid);
#if PG_VERSION_NUM < 150000
static void send_relation_and_attrs(Relation relation, TransactionId xid,
									LogicalDecodingContext *ctx);
static void maybe_send_schema(LogicalDecodingContext *ctx,
				  			  ReorderBufferTXN *txn, ReorderBufferChange *change,
				  			  Relation relation, RelationSyncEntry *relentry);
static RelationSyncEntry *get_rel_sync_entry(BilogicalData *data, Oid relid);
#else
static void send_relation_and_attrs(Relation relation, TransactionId xid,
									LogicalDecodingContext *ctx,
									Bitmapset *columns);
static void maybe_send_schema(LogicalDecodingContext *ctx,
				  			  ReorderBufferChange *change,
				  			  Relation relation, RelationSyncEntry *relentry);
static RelationSyncEntry *get_rel_sync_entry(BilogicalData *data, Relation relation);
#endif
static void rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue);
#if PG_VERSION_NUM < 150000
static void update_replication_progress(LogicalDecodingContext *ctx);
#else
static void update_replication_progress(LogicalDecodingContext *ctx, bool skipped_xact);
#endif
static void send_repl_origin(LogicalDecodingContext *ctx, RepOriginId origin_id,
				 			 XLogRecPtr origin_lsn, bool send_origin);
#if PG_VERSION_NUM >= 150000
static void send_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static EState *create_estate_for_relation(Relation rel);
static bool row_filter_exec_expr(ExprState *state, ExprContext *econtext);
static void ensure_entry_cxt(BilogicalData *data, RelationSyncEntry *entry);
static void row_filter_init(BilogicalData *data, List *publications,
							RelationSyncEntry *entry);
static void column_list_init(BilogicalData *data, List *publications,
				 			 RelationSyncEntry *entry);
static void init_tuple_slot(BilogicalData *data, Relation relation,
							RelationSyncEntry *entry);
static bool row_filter(Relation relation, TupleTableSlot *old_slot,
		   			   TupleTableSlot **new_slot_ptr, RelationSyncEntry *entry,
		   			   ReorderBufferChangeType *action);
#endif

/*
 * global parameters definition:
 *
 * - bilogical.origin: none | any
 * - bilogical.two_phase: true | false
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable("bilogical.two_phase",
							 "Enable two-phase transactions.",
							 NULL,
							 &bilogical_twophase,
							 false,
							 PGC_POSTMASTER,
							 GUC_NOT_IN_SAMPLE,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("bilogical.origin",
							   "Specifies whether the subscription will request the publisher to only \
							 	send changes that don't have an origin or send changes regardless of origin.",
							   NULL,
							   &bilogical_origin,
							   BILOGICAL_ORIGIN_NONE,
							   PGC_POSTMASTER,
							   GUC_NOT_IN_SAMPLE,
							   NULL,
							   NULL,
							   NULL);
}

/* register output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = bill_startup;
	cb->begin_cb = bill_begin;
	cb->change_cb = bill_change;
	cb->truncate_cb = bill_truncate;
	cb->commit_cb = bill_commit;
	cb->message_cb = bill_message;
	cb->filter_by_origin_cb = bill_filter_by_origin;
	cb->shutdown_cb = bill_shutdown;

	/* TODO: support prepared transaction for 14 and blow */
#if PG_VERSION_NUM >= 150000
	/* cb->filter_prepare_cb = bill_filter_prepare */
	cb->begin_prepare_cb = bill_begin_prepare;
	cb->prepare_cb = bill_prepare;
	cb->commit_prepared_cb = bill_commit_prepared;
	cb->rollback_prepared_cb = bill_rollback_prepared;
	cb->stream_prepare_cb = bill_stream_prepare;
#endif

	/* streaming mode */
	cb->stream_start_cb = bill_stream_start;
	cb->stream_stop_cb = bill_stream_stop;
	cb->stream_abort_cb = bill_stream_abort;
	cb->stream_commit_cb = bill_stream_commit;
	cb->stream_change_cb = bill_stream_change;
	cb->stream_message_cb = bill_stream_message;
	cb->stream_truncate_cb = bill_stream_truncate;
}



/*
 * LogicalDecodeStartupCB
 */
static void
bill_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
			 bool is_init)
{
	BilogicalData *data = palloc0(sizeof(BilogicalData));
	static bool pubcb_registered = false;
	MemoryContextCallback *mctxcallback;

	data->context = AllocSetContextCreate(ctx->context,
										  "bilogical context",
										  ALLOCSET_DEFAULT_SIZES);

	Assert(BillEState.ctxPub == NULL);
	BillEState.ctxPub = AllocSetContextCreate(ctx->context,
								   			  "bilogical publication context",
								   			  ALLOCSET_SMALL_SIZES);

	Assert(BillEState.ctxCache == NULL);
	BillEState.ctxCache = AllocSetContextCreate(ctx->context,
									 			"bilogical cache context",
									 			ALLOCSET_SMALL_SIZES);

	mctxcallback = palloc0(sizeof(MemoryContextCallback));
	mctxcallback->func = bill_ctx_reset;
	MemoryContextRegisterResetCallback(ctx->context, mctxcallback);

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	/*
	 * Do initialization.
	 */
	if (!is_init)
	{
		parse_plugin_params(ctx->output_plugin_options, data);

		if (data->protocol_version > LOGICALREP_PROTO_VERSION_NUM15)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("client sent proto_version=%d but we only support protocol %d or lower",
							data->protocol_version, LOGICALREP_PROTO_VERSION_NUM15)));

		if (data->protocol_version < LOGICALREP_PROTO_VERSION_NUM13)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("client sent proto_version=%d but we only support protocol %d or higher",
							data->protocol_version, LOGICALREP_PROTO_VERSION_NUM13)));

		if (list_length(data->publication_names) < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("publication_names parameter missing")));

		if (!data->streaming)
			ctx->streaming = false;
		else if (data->protocol_version < LOGICALREP_PROTO_VERSION_NUM14)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("requested proto_version=%d does not support streaming, need %d or higher",
							data->protocol_version, LOGICALREP_PROTO_VERSION_NUM14)));
		else if (!ctx->streaming)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("streaming requested, but not supported by bilogical plugin")));

		BillEState.is_in_streaming = false;

		if (data->two_phase && data->protocol_version < LOGICALREP_PROTO_VERSION_NUM15)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("requested proto_version=%d does not support two-phase commit, need %d or higher",
							data->protocol_version, LOGICALREP_PROTO_VERSION_NUM15)));

#if PG_VERSION_NUM >= 150000
		if (!data->two_phase)
			ctx->twophase_opt_given = false;
		else if (!(ctx->twophase))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("two-phase commit requested, but not enabled by output plugin")));
		else
			ctx->twophase_opt_given = true;
#endif

		data->publications = NIL;
		BillEState.is_pub_valid = false;

		/*
		 * callback for pg_publication
		 */
		if (!pubcb_registered)
		{
			CacheRegisterSyscacheCallback(PUBLICATIONOID,
										  pub_invalidation_cb,
										  (Datum) 0);
			pubcb_registered = true;
		}

		init_relcache(CacheMemoryContext);
	}
	else
	{
		ctx->streaming = false;
		ctx->twophase = false;
	}
}

/*
 * LogicalDecodeBeginCB
 */
static void
bill_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
#if PG_VERSION_NUM < 150000
	bool		send_replication_origin = txn->origin_id != InvalidRepOriginId;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_begin(ctx->out, txn);

	send_repl_origin(ctx, txn->origin_id, txn->origin_lsn, send_replication_origin);

	OutputPluginWrite(ctx, true);
#else
	BilogicalTxnData *txndata = MemoryContextAllocZero(ctx->context,
													   sizeof(BilogicalTxnData));

	txn->output_plugin_private = txndata;
#endif
}

/*
 * LogicalDecodeChangeCB
 */
static void
bill_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				Relation relation, ReorderBufferChange *change)
{
	BilogicalData *data = (BilogicalData *) ctx->output_plugin_private;
	MemoryContext old;
	RelationSyncEntry *relentry;
	TransactionId xid = InvalidTransactionId;
	Relation	ancestor = NULL;
#if PG_VERSION_NUM >= 150000
	BilogicalTxnData *txndata = (BilogicalTxnData *) txn->output_plugin_private;
	Relation	targetrel = relation;
	ReorderBufferChangeType action = change->action;
	TupleTableSlot *old_slot = NULL;
	TupleTableSlot *new_slot = NULL;
#endif

#if PG_VERSION_NUM < 150000
	update_replication_progress(ctx);
#else
	update_replication_progress(ctx, false);
#endif

	if (!is_publishable_relation(relation))
		return;

	if (BillEState.is_in_streaming)
		xid = change->txn->xid;

#if PG_VERSION_NUM < 150000
	relentry = get_rel_sync_entry(data, RelationGetRelid(relation));
#else
	relentry = get_rel_sync_entry(data, relation);
#endif

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (!relentry->pubactions.pubinsert)
				return;
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if (!relentry->pubactions.pubupdate)
				return;
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (!relentry->pubactions.pubdelete)
				return;
			break;
		default:
			Assert(false);
	}

	old = MemoryContextSwitchTo(data->context);

#if PG_VERSION_NUM < 150000
	maybe_send_schema(ctx, txn, change, relation, relentry);
#endif

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
#if PG_VERSION_NUM < 150000
				HeapTuple	tuple = &change->data.tp.newtuple->tuple;
#else
				new_slot = relentry->new_slot;
				ExecStoreHeapTuple(&change->data.tp.newtuple->tuple,
							   	   new_slot, false);
#endif

				if (relentry->publish_as_relid != RelationGetRelid(relation))
				{
					Assert(relation->rd_rel->relispartition);
					ancestor = RelationIdGetRelation(relentry->publish_as_relid);

#if PG_VERSION_NUM < 150000
					relation = ancestor;

					if (relentry->map)
						tuple = execute_attr_map_tuple(tuple, relentry->map);
#else
					targetrel = ancestor;

					if (relentry->attrmap)
					{
						TupleDesc	tupdesc = RelationGetDescr(targetrel);

						new_slot = execute_attr_map_slot(relentry->attrmap,
														 new_slot,
														 MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));
					}
#endif
				}

#if PG_VERSION_NUM < 150000
				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_insert(ctx->out, xid, relation, tuple,
										data->binary);
				OutputPluginWrite(ctx, true);
#else
				if (!row_filter(targetrel, NULL, &new_slot, relentry,
									 &action))
				break;

				if (txndata && !txndata->sent_begin_txn)
					send_begin(ctx, txn);

				maybe_send_schema(ctx, change, relation, relentry);

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_insert(ctx->out, xid, targetrel, new_slot,
										data->binary, relentry->columns);
				OutputPluginWrite(ctx, true);
#endif
				break;
			}
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
#if PG_VERSION_NUM < 150000
				HeapTuple	oldtuple = change->data.tp.oldtuple ?
				&change->data.tp.oldtuple->tuple : NULL;
				HeapTuple	newtuple = &change->data.tp.newtuple->tuple;
#else
				if (change->data.tp.oldtuple)
				{
					old_slot = relentry->old_slot;
					ExecStoreHeapTuple(&change->data.tp.oldtuple->tuple,
									   old_slot, false);
				}

				new_slot = relentry->new_slot;
				ExecStoreHeapTuple(&change->data.tp.newtuple->tuple,
							   	   new_slot, false);
#endif
				if (relentry->publish_as_relid != RelationGetRelid(relation))
				{
					Assert(relation->rd_rel->relispartition);
					ancestor = RelationIdGetRelation(relentry->publish_as_relid);

#if PG_VERSION_NUM < 150000
					relation = ancestor;

					if (relentry->map)
					{
						if (oldtuple)
							oldtuple = execute_attr_map_tuple(oldtuple,
															  relentry->map);
						newtuple = execute_attr_map_tuple(newtuple,
														  relentry->map);
					}
#else
					targetrel = ancestor;

					if (relentry->attrmap)
					{
						TupleDesc tupdesc = RelationGetDescr(targetrel);

						if (old_slot)
							old_slot = execute_attr_map_slot(relentry->attrmap,
															 old_slot,
															 MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));

						new_slot = execute_attr_map_slot(relentry->attrmap,
														 new_slot,
														 MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));
					}
#endif
				}

#if PG_VERSION_NUM < 150000
				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_update(ctx->out, xid, relation, oldtuple,
										newtuple, data->binary);
				OutputPluginWrite(ctx, true);
#else
				if (!row_filter(targetrel, old_slot, &new_slot,
										 relentry, &action))
					break;

				/* Send BEGIN if we haven't yet */
				if (txndata && !txndata->sent_begin_txn)
					send_begin(ctx, txn);

				maybe_send_schema(ctx, change, relation, relentry);

				OutputPluginPrepareWrite(ctx, true);

				switch (action)
				{
					case REORDER_BUFFER_CHANGE_INSERT:
						logicalrep_write_insert(ctx->out, xid, targetrel,
												new_slot, data->binary,
												relentry->columns);
						break;
					case REORDER_BUFFER_CHANGE_UPDATE:
						logicalrep_write_update(ctx->out, xid, targetrel,
												old_slot, new_slot, data->binary,
												relentry->columns);
						break;
					case REORDER_BUFFER_CHANGE_DELETE:
						logicalrep_write_delete(ctx->out, xid, targetrel,
												old_slot, data->binary,
												relentry->columns);
						break;
					default:
						Assert(false);
				}

				OutputPluginWrite(ctx, true);
#endif
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
#if PG_VERSION_NUM < 150000
				HeapTuple	oldtuple = &change->data.tp.oldtuple->tuple;
#else
				old_slot = relentry->old_slot;

				ExecStoreHeapTuple(&change->data.tp.oldtuple->tuple,
								   old_slot, false);
#endif

				if (relentry->publish_as_relid != RelationGetRelid(relation))
				{
					Assert(relation->rd_rel->relispartition);
					ancestor = RelationIdGetRelation(relentry->publish_as_relid);

#if PG_VERSION_NUM < 150000
					relation = ancestor;

					if (relentry->map)
						oldtuple = execute_attr_map_tuple(oldtuple, relentry->map);
#else
					targetrel = ancestor;

					if (relentry->attrmap)
					{
						TupleDesc tupdesc = RelationGetDescr(targetrel);

						old_slot = execute_attr_map_slot(relentry->attrmap,
														 old_slot,
														 MakeTupleTableSlot(tupdesc, &TTSOpsVirtual));
					}
#endif
				}

#if PG_VERSION_NUM < 150000
				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_delete(ctx->out, xid, relation, oldtuple,
										data->binary);
				OutputPluginWrite(ctx, true);
#else
				if (!row_filter(targetrel, old_slot, &new_slot,
										 relentry, &action))
					break;

				if (txndata && !txndata->sent_begin_txn)
					send_begin(ctx, txn);

				maybe_send_schema(ctx, change, relation, relentry);

				OutputPluginPrepareWrite(ctx, true);
				logicalrep_write_delete(ctx->out, xid, targetrel,
										old_slot, data->binary,
										relentry->columns);
				OutputPluginWrite(ctx, true);
#endif
			}
			else
				elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
			break;
		default:
			Assert(false);
	}

	if (RelationIsValid(ancestor))
	{
		RelationClose(ancestor);
		ancestor = NULL;
	}

#if PG_VERSION_NUM >= 150000
	if (relentry->attrmap)
	{
		if (old_slot)
			ExecDropSingleTupleTableSlot(old_slot);

		if (new_slot)
			ExecDropSingleTupleTableSlot(new_slot);
	}
#endif

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}

/*
 * LogicalDecodeTruncateCB
 */
static void
bill_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  int nrelations, Relation relations[], ReorderBufferChange *change)
{
	BilogicalData *data = (BilogicalData *) ctx->output_plugin_private;
	MemoryContext old;
	RelationSyncEntry *relentry;
	int			i;
	int			nrelids;
	Oid		   *relids;
	TransactionId xid = InvalidTransactionId;
#if PG_VERSION_NUM >= 150000
	BilogicalTxnData *txndata = (BilogicalTxnData *)txn->output_plugin_private;
#endif

#if PG_VERSION_NUM < 150000
	update_replication_progress(ctx);
#else
	update_replication_progress(ctx, false);
#endif

	if (BillEState.is_in_streaming)
		xid = change->txn->xid;

	old = MemoryContextSwitchTo(data->context);

	relids = palloc0(nrelations * sizeof(Oid));
	nrelids = 0;

	for (i = 0; i < nrelations; i++)
	{
		Relation	relation = relations[i];
		Oid			relid = RelationGetRelid(relation);

		if (!is_publishable_relation(relation))
			continue;

#if PG_VERSION_NUM < 150000
		relentry = get_rel_sync_entry(data, relid);
#else
		relentry = get_rel_sync_entry(data, relation);
#endif

		if (!relentry->pubactions.pubtruncate)
			continue;

		if (relation->rd_rel->relispartition &&
			relentry->publish_as_relid != relid)
			continue;

		relids[nrelids++] = relid;

#if PG_VERSION_NUM >= 150000
		if (txndata && !txndata->sent_begin_txn)
			send_begin(ctx, txn);
#endif

#if PG_VERSION_NUM < 150000
		maybe_send_schema(ctx, txn, change, relation, relentry);
#else
		maybe_send_schema(ctx, change, relation, relentry);
#endif
	}

	if (nrelids > 0)
	{
		OutputPluginPrepareWrite(ctx, true);
		logicalrep_write_truncate(ctx->out,
								  xid,
								  nrelids,
								  relids,
								  change->data.truncate.cascade,
								  change->data.truncate.restart_seqs);
		OutputPluginWrite(ctx, true);
	}

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}


/*
 * LogicalDecodeCommitCB
 */
static void
bill_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			XLogRecPtr commit_lsn)
{
#if PG_VERSION_NUM < 150000
	update_replication_progress(ctx);
#else
	BilogicalTxnData	*txndata = (BilogicalTxnData *) txn->output_plugin_private;
	bool 				sent_begin_txn;

	Assert(txndata);

	sent_begin_txn = txndata->sent_begin_txn;
	update_replication_progress(ctx, !sent_begin_txn);
	pfree(txndata);
	txn->output_plugin_private = NULL;

	if (!sent_begin_txn)
	{
		elog(DEBUG1, "skipped replication of an empty transaction with XID: %u", txn->xid);
		return;
	}
#endif
	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_commit(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);
}


/*
 * LogicalDecodeMessageCB
 */
static void
bill_message(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 XLogRecPtr message_lsn, bool transactional, const char *prefix, Size sz,
				 const char *message)
{
	BilogicalData *data = (BilogicalData *) ctx->output_plugin_private;
	TransactionId xid = InvalidTransactionId;

#if PG_VERSION_NUM < 150000
	update_replication_progress(ctx);
#else
	update_replication_progress(ctx, false);
#endif

	if (!data->messages)
		return;

	if (BillEState.is_in_streaming)
		xid = txn->xid;

#if PG_VERSION_NUM >= 150000
	if (transactional)
	{
		BilogicalTxnData *txndata = (BilogicalTxnData *)txn->output_plugin_private;

		if (txndata && !txndata->sent_begin_txn)
			send_begin(ctx, txn);
	}
#endif

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_message(ctx->out,
							 xid,
							 message_lsn,
							 transactional,
							 prefix,
							 sz,
							 message);
	OutputPluginWrite(ctx, true);
}



/*
 * LogicalDecodeFilterByOriginCB
 */
static bool
bill_filter_by_origin(LogicalDecodingContext *ctx,
				   	  RepOriginId origin_id)
{
	BilogicalData *data = (BilogicalData *) ctx->output_plugin_private;

	if (data->origin && (pg_strcasecmp(data->origin, BILOGICAL_ORIGIN_NONE) == 0) &&
		origin_id != InvalidRepOriginId)
		return true;

	return false;
}


/*
 * LogicalDecodeShutdownCB
 */
static void
bill_shutdown(LogicalDecodingContext *ctx)
{
	if (BillEState.relcache)
	{
		hash_destroy(BillEState.relcache);
		BillEState.relcache = NULL;
	}

	BillEState.ctxPub = NULL;
	BillEState.ctxCache = NULL;
}


/*
 * LogicalDecodeStreamStartCB
 */
static void
bill_stream_start(struct LogicalDecodingContext *ctx,
					  ReorderBufferTXN *txn)
{
	bool		send_replication_origin = txn->origin_id != InvalidRepOriginId;

	Assert(!BillEState.is_in_streaming);

	if (rbtxn_is_streamed(txn))
		send_replication_origin = false;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_stream_start(ctx->out, txn->xid, !rbtxn_is_streamed(txn));

	send_repl_origin(ctx, txn->origin_id, 
					 InvalidXLogRecPtr, send_replication_origin);

	OutputPluginWrite(ctx, true);

	BillEState.is_in_streaming = true;
}


/*
 * LogicalDecodeStreamStopCB
 */
static void
bill_stream_stop(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn)
{
	Assert(BillEState.is_in_streaming);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_stop(ctx->out);
	OutputPluginWrite(ctx, true);

	BillEState.is_in_streaming = false;
}

/*
 * LogicalDecodeStreamAbortCB
 */
static void
bill_stream_abort(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn,
				  XLogRecPtr abort_lsn)
{
	ReorderBufferTXN *toptxn;

	Assert(!BillEState.is_in_streaming);

	toptxn = (txn->toptxn) ? txn->toptxn : txn;

	Assert(rbtxn_is_streamed(toptxn));

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_abort(ctx->out, toptxn->xid, txn->xid);
	OutputPluginWrite(ctx, true);

	cleanup_rel_sync_cache(toptxn->xid, false);
}

/*
 * LogicalDecodeStreamCommitCB
 */
static void
bill_stream_commit(LogicalDecodingContext *ctx,
				   ReorderBufferTXN *txn,
				   XLogRecPtr commit_lsn)
{
	Assert(!BillEState.is_in_streaming);
	Assert(rbtxn_is_streamed(txn));

#if PG_VERSION_NUM < 150000
	update_replication_progress(ctx);
#else
	update_replication_progress(ctx, false);
#endif

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_commit(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);

	cleanup_rel_sync_cache(txn->xid, true);
}

/*
 * LogicalDecodeStreamChangeCB
 */
static void 
bill_stream_change(LogicalDecodingContext *ctx,
				   ReorderBufferTXN *txn,
				   Relation relation,
				   ReorderBufferChange *change)
{
	bill_change(ctx, txn, relation, change);
}


/*
 * LogicalDecodeStreamMessageCB
 */
static void
bill_stream_message(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn,
					XLogRecPtr message_lsn,
					bool transactional,
					const char *prefix,
					Size message_size,
					const char *message)
{
	bill_message(ctx, txn, message_lsn, transactional, prefix, message_size, message);
}


/*
 * LogicalDecodeStreamTruncateCB
 */
static void
bill_stream_truncate(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn,
					 int nrelations,
					 Relation relations[],
					 ReorderBufferChange *change)
{
	bill_truncate(ctx, txn, nrelations, relations, change);
}

#if PG_VERSION_NUM >= 150000
/*
 * static bool
 * bill_filter_prepare(LogicalDecodingContext *ctx,
 * 					TransactionId xid,
 * 				    const char *gid)
 * {
 * }
 */
static void
bill_begin_prepare(LogicalDecodingContext *ctx,
				   ReorderBufferTXN *txn)
{
	bool send_replication_origin = txn->origin_id != InvalidRepOriginId;

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_begin_prepare(ctx->out, txn);

	send_repl_origin(ctx, txn->origin_id, txn->origin_lsn,
					 send_replication_origin);

	OutputPluginWrite(ctx, true);
}

static void
bill_prepare(LogicalDecodingContext *ctx,
			 ReorderBufferTXN *txn,
			 XLogRecPtr prepare_lsn)
{
	update_replication_progress(ctx, false);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_prepare(ctx->out, txn, prepare_lsn);
	OutputPluginWrite(ctx, true);
}

static void
bill_commit_prepared(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	update_replication_progress(ctx, false);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_commit_prepared(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);
}

static void
bill_rollback_prepared(LogicalDecodingContext *ctx,
					   ReorderBufferTXN *txn,
					   XLogRecPtr prepare_end_lsn,
					   TimestampTz prepare_time)
{
	update_replication_progress(ctx, false);

	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_rollback_prepared(ctx->out, txn, prepare_end_lsn,
									   prepare_time);
	OutputPluginWrite(ctx, true);
}

static void
bill_stream_prepare(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn,
					XLogRecPtr prepare_lsn)
{
	Assert(rbtxn_is_streamed(txn));

	update_replication_progress(ctx, false);
	OutputPluginPrepareWrite(ctx, true);
	logicalrep_write_stream_prepare(ctx->out, txn, prepare_lsn);
	OutputPluginWrite(ctx, true);
}
#endif


/* 
 * -------------------------------------------------------------------------------------------------
 * Help routines
 * -------------------------------------------------------------------------------------------------
 *
 * Parse parameters
 */
static void parse_plugin_params(List *options, BilogicalData *data)
{
	ListCell   *lc;
	bool		protocol_version_given = false;
	bool		publication_names_given = false;
	bool		binary_option_given = false;
	bool		messages_option_given = false;
	bool		streaming_given = false;
#if PG_VERSION_NUM >= 150000
	bool two_phase_option_given = false;
#endif

	data->binary = false;
	data->streaming = false;
	data->messages = false;
	data->two_phase = false;


	foreach(lc, options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		Assert(defel->arg == NULL || IsA(defel->arg, String));

		/* Check each param, whether or not we recognize it */
		if (strcmp(defel->defname, "proto_version") == 0)
		{
			int64		parsed;
#if PG_VERSION_NUM >= 150000
			char	   *endptr;
#endif

			if (protocol_version_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			protocol_version_given = true;

#if PG_VERSION_NUM < 150000
			if (!scanint8(strVal(defel->arg), true, &parsed))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid proto_version")));
#else
			errno = 0;
			parsed = strtoul(strVal(defel->arg), &endptr, 10);
			if (errno != 0 || *endptr != '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid proto_version")));
#endif
			if (parsed > PG_UINT32_MAX || parsed < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("proto_version \"%s\" out of range",
								strVal(defel->arg))));

			data->protocol_version = (uint32) parsed;
		}
		else if (strcmp(defel->defname, "publication_names") == 0)
		{
			if (publication_names_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			publication_names_given = true;

			if (!SplitIdentifierString(strVal(defel->arg), ',',
									   &data->publication_names))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("invalid publication_names syntax")));
		}
		else if (strcmp(defel->defname, "binary") == 0)
		{
			if (binary_option_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			binary_option_given = true;

			data->binary = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "messages") == 0)
		{
			if (messages_option_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			messages_option_given = true;

			data->messages = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "streaming") == 0)
		{
			if (streaming_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			streaming_given = true;

			data->streaming = defGetBoolean(defel);
		}
#if PG_VERSION_NUM >= 150000
		else if (strcmp(defel->defname, "two_phase") == 0)
		{
			if (two_phase_option_given)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			two_phase_option_given = true;

			data->two_phase = defGetBoolean(defel);
		}
#endif
		else
			elog(ERROR, "unrecognized bilogical option: %s", defel->defname);
	}

	if (pg_strcasecmp(bilogical_origin, BILOGICAL_ORIGIN_NONE) != 0 &&
		pg_strcasecmp(bilogical_origin, BILOGICAL_ORIGIN_ANY) != 0)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("unrecognized origin value: \"%s\"", bilogical_origin));

	data->origin = pstrdup(bilogical_origin);
#if PG_VERSION_NUM < 150000
	data->two_phase = bilogical_twophase;
#endif
}

/*
 * Callback of memory context cleanup
 */
static void
bill_ctx_reset(void *arg)
{
	BillEState.ctxPub = NULL;
	BillEState.ctxCache = NULL;
}

static void
send_repl_origin(LogicalDecodingContext *ctx, RepOriginId origin_id,
				 XLogRecPtr origin_lsn, bool send_origin)
{
	if (send_origin)
	{
		char	   *origin;

		if (replorigin_by_oid(origin_id, true, &origin))
		{
			OutputPluginWrite(ctx, false);
			OutputPluginPrepareWrite(ctx, true);
			logicalrep_write_origin(ctx->out, origin, origin_lsn);
		}
	}
}

/*
 * Send BEGIN
 */
#if PG_VERSION_NUM >= 150000
static void
send_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	bool		send_replication_origin = txn->origin_id != InvalidRepOriginId;
	BilogicalTxnData *txndata = (BilogicalTxnData *) txn->output_plugin_private;

	Assert(txndata);
	Assert(!txndata->sent_begin_txn);

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	logicalrep_write_begin(ctx->out, txn);
	txndata->sent_begin_txn = true;

	send_repl_origin(ctx, txn->origin_id, txn->origin_lsn,
					 send_replication_origin);

	OutputPluginWrite(ctx, true);
}
#endif


static void
#if PG_VERSION_NUM < 150000
maybe_send_schema(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn, ReorderBufferChange *change,
				  Relation relation, RelationSyncEntry *relentry)
#else
maybe_send_schema(LogicalDecodingContext *ctx,
				  ReorderBufferChange *change,
				  Relation relation, RelationSyncEntry *relentry)
#endif
{
	bool		schema_sent;
	TransactionId xid = InvalidTransactionId;
	TransactionId topxid = InvalidTransactionId;

	if (BillEState.is_in_streaming)
		xid = change->txn->xid;

	if (change->txn->toptxn)
		topxid = change->txn->toptxn->xid;
	else
		topxid = xid;

	if (BillEState.is_in_streaming)
		schema_sent = get_schema_sent_in_streamed_txn(relentry, topxid);
	else
		schema_sent = relentry->schema_sent;

	if (schema_sent)
		return;

	if (relentry->publish_as_relid != RelationGetRelid(relation))
	{
		Relation	ancestor = RelationIdGetRelation(relentry->publish_as_relid);
#if PG_VERSION_NUM < 150000
		TupleDesc	indesc = RelationGetDescr(relation);
		TupleDesc	outdesc = RelationGetDescr(ancestor);
		MemoryContext oldctx;

		oldctx = MemoryContextSwitchTo(BillEState.ctxCache);

		indesc = CreateTupleDescCopy(indesc);
		outdesc = CreateTupleDescCopy(outdesc);
		relentry->map = convert_tuples_by_name(indesc, outdesc);
		if (relentry->map == NULL)
		{
			FreeTupleDesc(indesc);
			FreeTupleDesc(outdesc);
		}

		MemoryContextSwitchTo(oldctx);
		send_relation_and_attrs(ancestor, xid, ctx);
		RelationClose(ancestor);
#else
		send_relation_and_attrs(ancestor, xid, ctx, relentry->columns);
		RelationClose(ancestor);
#endif
	}

#if PG_VERSION_NUM < 150000
	send_relation_and_attrs(relation, xid, ctx);
#else
	send_relation_and_attrs(relation, xid, ctx, relentry->columns);
#endif

	if (BillEState.is_in_streaming)
		set_schema_sent_in_streamed_txn(relentry, topxid);
	else
		relentry->schema_sent = true;
}


/*
 * Sends a relation
 */
static void
#if PG_VERSION_NUM < 150000
send_relation_and_attrs(Relation relation, TransactionId xid,
						LogicalDecodingContext *ctx)
#else
send_relation_and_attrs(Relation relation, TransactionId xid,
						LogicalDecodingContext *ctx,
						Bitmapset *columns)
#endif
{
	TupleDesc	desc = RelationGetDescr(relation);
	int			i;

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (att->attisdropped || att->attgenerated)
			continue;

		if (att->atttypid < FirstGenbkiObjectId)
			continue;

#if PG_VERSION_NUM > 150000
		if (columns != NULL && !bms_is_member(att->attnum, columns))
			continue;
#endif

		OutputPluginPrepareWrite(ctx, false);
		logicalrep_write_typ(ctx->out, xid, att->atttypid);
		OutputPluginWrite(ctx, false);
	}

	OutputPluginPrepareWrite(ctx, false);
#if PG_VERSION_NUM < 150000
	logicalrep_write_rel(ctx->out, xid, relation);
#else
	logicalrep_write_rel(ctx->out, xid, relation, columns);
#endif
	OutputPluginWrite(ctx, false);
}




/*
 * Load publications from the list of publication names.
 */
static List *
LoadPublications(List *pubnames)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, pubnames)
	{
		char	   *pubname = (char *) lfirst(lc);
		Publication *pub = GetPublicationByName(pubname, false);

		result = lappend(result, pub);
	}

	return result;
}

/*
 * Publication cache invalidation callback.
 */
static void
pub_invalidation_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	BillEState.is_pub_valid = false;

	rel_sync_cache_publication_cb(arg, cacheid, hashvalue);
}

/*
 * init relcache
 */
static void
init_relcache(MemoryContext ctxCache)
{
	HASHCTL		ctl;
	static bool relcb_registered = false;

	if (BillEState.relcache != NULL)
		return;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RelationSyncEntry);
	ctl.hcxt = ctxCache;

	BillEState.relcache = hash_create("bilogical relcache",
									  128, &ctl,
									  HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	Assert(BillEState.relcache  != NULL);

	if (relcb_registered)
		return;

	CacheRegisterRelcacheCallback(rel_sync_cache_relation_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(PUBLICATIONRELMAP,
								  rel_sync_cache_publication_cb,
								  (Datum)0);
#if PG_VERSION_NUM >= 150000
	CacheRegisterSyscacheCallback(PUBLICATIONNAMESPACEMAP,
								  rel_sync_cache_publication_cb,
								  (Datum)0);
#endif

	relcb_registered = true;
}

/*
 * We expect relatively small number of streamed transactions.
 */
static bool
get_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid)
{
	ListCell   *lc;

	foreach(lc, entry->streamed_txns)
	{
		if (xid == (uint32) lfirst_int(lc))
			return true;
	}

	return false;
}

/*
 * Add the xid in the rel sync entry for which we have already sent the schema
 * of the relation.
 */
static void
set_schema_sent_in_streamed_txn(RelationSyncEntry *entry, TransactionId xid)
{
	MemoryContext oldctx;

	oldctx = MemoryContextSwitchTo(CacheMemoryContext);

	entry->streamed_txns = lappend_int(entry->streamed_txns, xid);

	MemoryContextSwitchTo(oldctx);
}


static RelationSyncEntry *
#if PG_VERSION_NUM < 150000
get_rel_sync_entry(BilogicalData *data, Oid relid)
#else
get_rel_sync_entry(BilogicalData *data, Relation relation)
#endif
{
	RelationSyncEntry *entry;
	bool		found;
	MemoryContext oldctx;
#if PG_VERSION_NUM >= 150000
	Oid			relid = RelationGetRelid(relation);
#endif

	Assert(BillEState.relcache != NULL);

	entry = (RelationSyncEntry *) hash_search(BillEState.relcache,
											  (void *) &relid,
											  HASH_ENTER, &found);
	Assert(entry != NULL);

	if (!found)
	{
#if PG_VERSION_NUM < 150000
		entry->schema_sent = false;
		entry->streamed_txns = NIL;
		entry->replicate_valid = false;
		entry->pubactions.pubinsert = entry->pubactions.pubupdate =
			entry->pubactions.pubdelete = entry->pubactions.pubtruncate = false;
		entry->publish_as_relid = InvalidOid;
		entry->map = NULL;
#else
		entry->replicate_valid = false;
		entry->schema_sent = false;
		entry->streamed_txns = NIL;
		entry->pubactions.pubinsert = entry->pubactions.pubupdate =
			entry->pubactions.pubdelete = entry->pubactions.pubtruncate = false;
		entry->new_slot = NULL;
		entry->old_slot = NULL;
		memset(entry->exprstate, 0, sizeof(entry->exprstate));
		entry->entry_cxt = NULL;
		entry->publish_as_relid = InvalidOid;
		entry->columns = NULL;
		entry->attrmap = NULL;
#endif
	}

	if (!entry->replicate_valid)
	{
		List	   *pubids = GetRelationPublications(relid);
		ListCell   *lc;
		Oid			publish_as_relid = relid;
		int			publish_ancestor_level = 0;
		bool		am_partition = get_rel_relispartition(relid);
		char		relkind = get_rel_relkind(relid);
#if PG_VERSION_NUM >= 150000
		Oid			schemaId = get_rel_namespace(relid);
		List	   *schemaPubids = GetSchemaPublications(schemaId);
		List	   *rel_publications = NIL;
#endif

		if (!BillEState.is_pub_valid)
		{
			Assert(BillEState.ctxPub);

			MemoryContextReset(BillEState.ctxPub);
			oldctx = MemoryContextSwitchTo(BillEState.ctxPub);

			data->publications = LoadPublications(data->publication_names);
			MemoryContextSwitchTo(oldctx);
			BillEState.is_pub_valid = true;
		}

#if PG_VERSION_NUM >= 150000
		entry->schema_sent = false;
		list_free(entry->streamed_txns);
		entry->streamed_txns = NIL;
		bms_free(entry->columns);
		entry->columns = NULL;
		entry->pubactions.pubinsert = false;
		entry->pubactions.pubupdate = false;
		entry->pubactions.pubdelete = false;
		entry->pubactions.pubtruncate = false;

		if (entry->old_slot)
		{
			TupleDesc	desc = entry->old_slot->tts_tupleDescriptor;

			Assert(desc->tdrefcount == -1);

			ExecDropSingleTupleTableSlot(entry->old_slot);
			FreeTupleDesc(desc);
		}
		if (entry->new_slot)
		{
			TupleDesc	desc = entry->new_slot->tts_tupleDescriptor;

			Assert(desc->tdrefcount == -1);

			ExecDropSingleTupleTableSlot(entry->new_slot);
			FreeTupleDesc(desc);
		}

		entry->old_slot = NULL;
		entry->new_slot = NULL;

		if (entry->attrmap)
			free_attrmap(entry->attrmap);
		entry->attrmap = NULL;

		if (entry->entry_cxt)
			MemoryContextDelete(entry->entry_cxt);

		entry->entry_cxt = NULL;
		entry->estate = NULL;
		memset(entry->exprstate, 0, sizeof(entry->exprstate));
#endif

		foreach(lc, data->publications)
		{
			Publication *pub = lfirst(lc);
			bool		publish = false;
			Oid			pub_relid = relid;
			int			ancestor_level = 0;

			if (pub->alltables)
			{
				publish = true;
				if (pub->pubviaroot && am_partition)
				{
					List	   *ancestors = get_partition_ancestors(relid);

					pub_relid = llast_oid(ancestors);
					ancestor_level = list_length(ancestors);
				}
			}

			if (!publish)
			{
				bool		ancestor_published = false;

				if (am_partition)
				{
					List	   *ancestors = get_partition_ancestors(relid);
					int			level = 0;

#if PG_VERSION_NUM < 150000
					ListCell   *lc2;

					foreach(lc2, ancestors)
					{
						Oid			ancestor = lfirst_oid(lc2);

						level++;

						if (list_member_oid(GetRelationPublications(ancestor),
											pub->oid))
						{
							ancestor_published = true;
							if (pub->pubviaroot)
							{
								pub_relid = ancestor;
								ancestor_level = level;
							}
						}
					}
#else
					Oid			ancestor;

					ancestor = GetTopMostAncestorInPublication(pub->oid,
															   ancestors,
															   &level);
				
					if (ancestor != InvalidOid)
					{
						ancestor_published = true;
						if (pub->pubviaroot)
						{
							pub_relid = ancestor;
							ancestor_level = level;
						}
					}
#endif
				}
#if PG_VERSION_NUM < 150000
				if (list_member_oid(pubids, pub->oid) || ancestor_published)
					publish = true;
#else
				if (list_member_oid(pubids, pub->oid) ||
					list_member_oid(schemaPubids, pub->oid) ||
					ancestor_published)
					publish = true;
#endif
			}

			if (publish &&
				(relkind != RELKIND_PARTITIONED_TABLE || pub->pubviaroot))
			{
				entry->pubactions.pubinsert |= pub->pubactions.pubinsert;
				entry->pubactions.pubupdate |= pub->pubactions.pubupdate;
				entry->pubactions.pubdelete |= pub->pubactions.pubdelete;
				entry->pubactions.pubtruncate |= pub->pubactions.pubtruncate;

				if (publish_ancestor_level > ancestor_level)
					continue;

#if PG_VERSION_NUM < 150000
				publish_as_relid = pub_relid;
				publish_ancestor_level = ancestor_level;
#else
				if (publish_ancestor_level < ancestor_level)
				{
					publish_as_relid = pub_relid;
					publish_ancestor_level = ancestor_level;
					rel_publications = NIL;
				}
				else
				{
					Assert(publish_as_relid == pub_relid);
				}

				rel_publications = lappend(rel_publications, pub);
#endif
			}
		}

#if PG_VERSION_NUM < 150000
		list_free(pubids);

		entry->publish_as_relid = publish_as_relid;
		entry->replicate_valid = true;
#else
		entry->publish_as_relid = publish_as_relid;

		if (entry->pubactions.pubinsert || entry->pubactions.pubupdate ||
			entry->pubactions.pubdelete)
		{
			init_tuple_slot(data, relation, entry);
			row_filter_init(data, rel_publications, entry);
			column_list_init(data, rel_publications, entry);
		}

		list_free(pubids);
		list_free(schemaPubids);
		list_free(rel_publications);

		entry->replicate_valid = true;
#endif
	}

	return entry;
}

/*
 * Cleanup relcache
 */
static void
cleanup_rel_sync_cache(TransactionId xid, bool is_commit)
{
	HASH_SEQ_STATUS hash_seq;
	RelationSyncEntry *entry;
	ListCell   *lc;

	Assert(BillEState.relcache != NULL);

	hash_seq_init(&hash_seq, BillEState.relcache);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		foreach(lc, entry->streamed_txns)
		{
			if (xid == (uint32) lfirst_int(lc))
			{
				if (is_commit)
					entry->schema_sent = true;

				entry->streamed_txns =
					foreach_delete_current(entry->streamed_txns, lc);
				break;
			}
		}
	}
}

/*
 * Relcache invalidation callback
 */
static void
rel_sync_cache_relation_cb(Datum arg, Oid relid)
{
	RelationSyncEntry *entry;

	if (BillEState.relcache == NULL)
		return;

#if PG_VERSION_NUM < 150000
	entry = (RelationSyncEntry *) hash_search(BillEState.relcache, &relid,
											  HASH_FIND, NULL);

	if (entry != NULL)
	{
		entry->schema_sent = false;
		list_free(entry->streamed_txns);
		entry->streamed_txns = NIL;
		if (entry->map)
		{
			FreeTupleDesc(entry->map->indesc);
			FreeTupleDesc(entry->map->outdesc);
			free_conversion_map(entry->map);
		}
		entry->map = NULL;
	}
#else
	if (OidIsValid(relid))
	{
		entry = (RelationSyncEntry *)hash_search(BillEState.relcache, &relid,
												 HASH_FIND, NULL);
		if (entry != NULL)
			entry->replicate_valid = false;
	}
	else
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, BillEState.relcache);
		while ((entry = (RelationSyncEntry *)hash_seq_search(&status)) != NULL)
		{
			entry->replicate_valid = false;
		}
	}
#endif
}

/*
 * Publication relation map syscache invalidation callback
 */
static void
rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	RelationSyncEntry *entry;

	if (BillEState.relcache == NULL)
		return;

	hash_seq_init(&status, BillEState.relcache);
	while ((entry = (RelationSyncEntry *) hash_seq_search(&status)) != NULL)
	{
		entry->replicate_valid = false;
#if PG_VERSION_NUM < 150000
		entry->pubactions.pubinsert = false;
		entry->pubactions.pubupdate = false;
		entry->pubactions.pubdelete = false;
		entry->pubactions.pubtruncate = false;
#endif
	}
}

static void
#if PG_VERSION_NUM < 150000
update_replication_progress(LogicalDecodingContext *ctx)
#else
update_replication_progress(LogicalDecodingContext *ctx, bool skipped_xact)
#endif
{
	static int	changes_count = 0;

#define CHANGES_THRESHOLD 100

	if (ctx->end_xact || ++changes_count >= CHANGES_THRESHOLD)
	{
#if PG_VERSION_NUM < 150000
		OutputPluginUpdateProgress(ctx);
#else
		OutputPluginUpdateProgress(ctx, skipped_xact);
#endif
		changes_count = 0;
	}
}

#if PG_VERSION_NUM >= 150000
static EState *
create_estate_for_relation(Relation rel)
{
	EState *estate;
	RangeTblEntry *rte;

	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;
	ExecInitRangeTable(estate, list_make1(rte));

	estate->es_output_cid = GetCurrentCommandId(false);

	return estate;
}

static bool
row_filter_exec_expr(ExprState *state, ExprContext *econtext)
{
	Datum ret;
	bool isnull;

	Assert(state != NULL);

	ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

	elog(DEBUG3, "row filter evaluates to %s (isnull: %s)",
		 isnull ? "false" : DatumGetBool(ret) ? "true"
											  : "false",
		 isnull ? "true" : "false");

	if (isnull)
		return false;

	return DatumGetBool(ret);
}

static void
ensure_entry_cxt(BilogicalData *data, RelationSyncEntry *entry)
{
	Relation relation;

	if (entry->entry_cxt)
		return;

	relation = RelationIdGetRelation(entry->publish_as_relid);

	entry->entry_cxt = AllocSetContextCreate(BillEState.ctxCache,
											 "entry private context",
											 ALLOCSET_SMALL_SIZES);

	MemoryContextCopyAndSetIdentifier(entry->entry_cxt,
									  RelationGetRelationName(relation));
}

static void
row_filter_init(BilogicalData *data, List *publications,
				RelationSyncEntry *entry)
{
	ListCell *lc;
	List *rfnodes[] = {NIL, NIL, NIL};
	bool no_filter[] = {false, false, false};
	MemoryContext oldctx;
	int idx;
	bool has_filter = true;
	Oid schemaid = get_rel_namespace(entry->publish_as_relid);

	foreach (lc, publications)
	{
		Publication *pub = lfirst(lc);
		HeapTuple rftuple = NULL;
		Datum rfdatum = 0;
		bool pub_no_filter = true;

		if (!pub->alltables &&
			!SearchSysCacheExists2(PUBLICATIONNAMESPACEMAP,
								   ObjectIdGetDatum(schemaid),
								   ObjectIdGetDatum(pub->oid)))
		{
			rftuple = SearchSysCache2(PUBLICATIONRELMAP,
									  ObjectIdGetDatum(entry->publish_as_relid),
									  ObjectIdGetDatum(pub->oid));

			if (HeapTupleIsValid(rftuple))
			{
				rfdatum = SysCacheGetAttr(PUBLICATIONRELMAP, rftuple,
										  Anum_pg_publication_rel_prqual,
										  &pub_no_filter);
			}
		}

		if (pub_no_filter)
		{
			if (rftuple)
				ReleaseSysCache(rftuple);

			no_filter[PUBACTION_INSERT] |= pub->pubactions.pubinsert;
			no_filter[PUBACTION_UPDATE] |= pub->pubactions.pubupdate;
			no_filter[PUBACTION_DELETE] |= pub->pubactions.pubdelete;

			if (no_filter[PUBACTION_INSERT] &&
				no_filter[PUBACTION_UPDATE] &&
				no_filter[PUBACTION_DELETE])
			{
				has_filter = false;
				break;
			}

			continue;
		}

		if (pub->pubactions.pubinsert && !no_filter[PUBACTION_INSERT])
			rfnodes[PUBACTION_INSERT] = lappend(rfnodes[PUBACTION_INSERT],
												TextDatumGetCString(rfdatum));
		if (pub->pubactions.pubupdate && !no_filter[PUBACTION_UPDATE])
			rfnodes[PUBACTION_UPDATE] = lappend(rfnodes[PUBACTION_UPDATE],
												TextDatumGetCString(rfdatum));
		if (pub->pubactions.pubdelete && !no_filter[PUBACTION_DELETE])
			rfnodes[PUBACTION_DELETE] = lappend(rfnodes[PUBACTION_DELETE],
												TextDatumGetCString(rfdatum));

		ReleaseSysCache(rftuple);
	}

	for (idx = 0; idx < NUM_ROWFILTER_PUBACTIONS; idx++)
	{
		if (no_filter[idx])
		{
			list_free_deep(rfnodes[idx]);
			rfnodes[idx] = NIL;
		}
	}

	if (has_filter)
	{
		Relation relation = RelationIdGetRelation(entry->publish_as_relid);

		ensure_entry_cxt(data, entry);

		oldctx = MemoryContextSwitchTo(entry->entry_cxt);
		entry->estate = create_estate_for_relation(relation);
		for (idx = 0; idx < NUM_ROWFILTER_PUBACTIONS; idx++)
		{
			List *filters = NIL;
			Expr *rfnode;

			if (rfnodes[idx] == NIL)
				continue;

			foreach (lc, rfnodes[idx])
				filters = lappend(filters, stringToNode((char *)lfirst(lc)));

			rfnode = make_orclause(filters);
			entry->exprstate[idx] = ExecPrepareExpr(rfnode, entry->estate);
		}
		MemoryContextSwitchTo(oldctx);

		RelationClose(relation);
	}
}

static void
column_list_init(BilogicalData *data, List *publications,
				 RelationSyncEntry *entry)
{
	ListCell *lc;
	bool first = true;
	Relation relation = RelationIdGetRelation(entry->publish_as_relid);

	foreach (lc, publications)
	{
		Publication *pub = lfirst(lc);
		HeapTuple cftuple = NULL;
		Datum cfdatum = 0;
		Bitmapset *cols = NULL;

		if (!pub->alltables)
		{
			bool pub_no_list = true;

			cftuple = SearchSysCache2(PUBLICATIONRELMAP,
									  ObjectIdGetDatum(entry->publish_as_relid),
									  ObjectIdGetDatum(pub->oid));

			if (HeapTupleIsValid(cftuple))
			{
				cfdatum = SysCacheGetAttr(PUBLICATIONRELMAP, cftuple,
										  Anum_pg_publication_rel_prattrs,
										  &pub_no_list);

				if (!pub_no_list)
				{
					ensure_entry_cxt(data, entry);

					cols = pub_collist_to_bitmapset(cols, cfdatum,
													entry->entry_cxt);

					if (bms_num_members(cols) == RelationGetNumberOfAttributes(relation))
					{
						bms_free(cols);
						cols = NULL;
					}
				}

				ReleaseSysCache(cftuple);
			}
		}

		if (first)
		{
			entry->columns = cols;
			first = false;
		}
		else if (!bms_equal(entry->columns, cols))
			ereport(ERROR,
					errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cannot use different column lists for table \"%s.%s\" in different publications",
						   get_namespace_name(RelationGetNamespace(relation)),
						   RelationGetRelationName(relation)));
	}

	RelationClose(relation);
}

static void
init_tuple_slot(BilogicalData *data, Relation relation,
				RelationSyncEntry *entry)
{
	MemoryContext oldctx;
	TupleDesc oldtupdesc;
	TupleDesc newtupdesc;

	oldctx = MemoryContextSwitchTo(BillEState.ctxCache);

	oldtupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));
	newtupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));

	entry->old_slot = MakeSingleTupleTableSlot(oldtupdesc, &TTSOpsHeapTuple);
	entry->new_slot = MakeSingleTupleTableSlot(newtupdesc, &TTSOpsHeapTuple);

	MemoryContextSwitchTo(oldctx);

	if (entry->publish_as_relid != RelationGetRelid(relation))
	{
		Relation ancestor = RelationIdGetRelation(entry->publish_as_relid);
		TupleDesc indesc = RelationGetDescr(relation);
		TupleDesc outdesc = RelationGetDescr(ancestor);

		oldctx = MemoryContextSwitchTo(BillEState.ctxCache);

		entry->attrmap = build_attrmap_by_name_if_req(indesc, outdesc);

		MemoryContextSwitchTo(oldctx);
		RelationClose(ancestor);
	}
}

static bool
row_filter(Relation relation, TupleTableSlot *old_slot,
		   TupleTableSlot **new_slot_ptr, RelationSyncEntry *entry,
		   ReorderBufferChangeType *action)
{
	TupleDesc desc;
	int i;
	bool old_matched,
		new_matched,
		result;
	TupleTableSlot *tmp_new_slot;
	TupleTableSlot *new_slot = *new_slot_ptr;
	ExprContext *ecxt;
	ExprState *filter_exprstate;

	static const int map_changetype_pubaction[] = {
		[REORDER_BUFFER_CHANGE_INSERT] = PUBACTION_INSERT,
		[REORDER_BUFFER_CHANGE_UPDATE] = PUBACTION_UPDATE,
		[REORDER_BUFFER_CHANGE_DELETE] = PUBACTION_DELETE};

	Assert(*action == REORDER_BUFFER_CHANGE_INSERT ||
		   *action == REORDER_BUFFER_CHANGE_UPDATE ||
		   *action == REORDER_BUFFER_CHANGE_DELETE);

	Assert(new_slot || old_slot);

	filter_exprstate = entry->exprstate[map_changetype_pubaction[*action]];

	if (!filter_exprstate)
		return true;

	elog(DEBUG3, "table \"%s.%s\" has row filter",
		 get_namespace_name(RelationGetNamespace(relation)),
		 RelationGetRelationName(relation));

	ResetPerTupleExprContext(entry->estate);

	ecxt = GetPerTupleExprContext(entry->estate);

	if (!new_slot || !old_slot)
	{
		ecxt->ecxt_scantuple = new_slot ? new_slot : old_slot;
		result = row_filter_exec_expr(filter_exprstate, ecxt);

		return result;
	}

	Assert(map_changetype_pubaction[*action] == PUBACTION_UPDATE);

	slot_getallattrs(new_slot);
	slot_getallattrs(old_slot);

	tmp_new_slot = NULL;
	desc = RelationGetDescr(relation);

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		if (new_slot->tts_isnull[i] || old_slot->tts_isnull[i])
			continue;

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_ONDISK(new_slot->tts_values[i]) &&
			!VARATT_IS_EXTERNAL_ONDISK(old_slot->tts_values[i]))
		{
			if (!tmp_new_slot)
			{
				tmp_new_slot = MakeSingleTupleTableSlot(desc, &TTSOpsVirtual);
				ExecClearTuple(tmp_new_slot);

				memcpy(tmp_new_slot->tts_values, new_slot->tts_values,
					   desc->natts * sizeof(Datum));
				memcpy(tmp_new_slot->tts_isnull, new_slot->tts_isnull,
					   desc->natts * sizeof(bool));
			}

			tmp_new_slot->tts_values[i] = old_slot->tts_values[i];
			tmp_new_slot->tts_isnull[i] = old_slot->tts_isnull[i];
		}
	}

	ecxt->ecxt_scantuple = old_slot;
	old_matched = row_filter_exec_expr(filter_exprstate, ecxt);

	if (tmp_new_slot)
	{
		ExecStoreVirtualTuple(tmp_new_slot);
		ecxt->ecxt_scantuple = tmp_new_slot;
	}
	else
		ecxt->ecxt_scantuple = new_slot;

	new_matched = row_filter_exec_expr(filter_exprstate, ecxt);

	if (!old_matched && !new_matched)
		return false;

	if (!old_matched && new_matched)
	{
		*action = REORDER_BUFFER_CHANGE_INSERT;

		if (tmp_new_slot)
			*new_slot_ptr = tmp_new_slot;
	}

	else if (old_matched && !new_matched)
		*action = REORDER_BUFFER_CHANGE_DELETE;

	return true;
}
#endif