USE [Genius3]
GO
/****** Object:  StoredProcedure [GENIUS3].[PROCESS_IMP_DATA]    Script Date: 21.04.2025 08:32:19 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--------------------PROCESS_IMP_DATA----------------------------------


ALTER PROCEDURE [GENIUS3].[PROCESS_IMP_DATA]

@prmSTORE_NO Numeric(19,0),
--cilem
@prmWithoutPrice Numeric(19,0)

AS 
	BEGIN

	IF EXISTS(SELECT 1 FROM STORE WHERE ID = @prmSTORE_NO)
	BEGIN
		SELECT @prmSTORE_NO = NUM FROM STORE WHERE ID = @prmSTORE_NO
	END
	ELSE
	BEGIN
		SET @prmSTORE_NO = 0
	END
	
	BEGIN TRANSACTION
		SET NOCOUNT ON
		SET CONCAT_NULL_YIELDS_NULL OFF
			
		DECLARE @adv_sql_rowcount INT 
		DECLARE @prmSTORE_ID NUMERIC(19)
		DECLARE @adv_error INT
		DECLARE @Ifound BIT  --created for isdeleted function to procedure conversion

		DECLARE @seqSC                                    NUMERIC(19) 
		DECLARE @seqSCE                                   NUMERIC(19) 
		DECLARE @seqSB                                    NUMERIC(19) 
		DECLARE @seqSP                                    NUMERIC(19) 
		DECLARE @seqSCP                                   NUMERIC(19)
		DECLARE @seqSCD                                   NUMERIC(19) 
		DECLARE @seqSL                                    NUMERIC(19) 
		DECLARE @seqSSP                                   NUMERIC(19)
		DECLARE @seqSS                                    NUMERIC(19) 
		DECLARE @vvokBarcodeCount                         NUMERIC(19) 
		DECLARE @vverrBarcodeCount                        NUMERIC(19) 
		DECLARE @vvokPriceCount                           NUMERIC(19) 
		DECLARE @vverrPriceCount                          NUMERIC(19) 
		DECLARE @isError                                  NUMERIC(1) 
		DECLARE @prevStockID                              NUMERIC(19) 
		DECLARE @prevStockCode                            NVARCHAR(24) 
		DECLARE @prevTableID                              NUMERIC(5) 
		DECLARE @prevBarcode                              NVARCHAR(80) 
		DECLARE @prevNum                                  NVARCHAR(20) 
		DECLARE @DataRec_processed                         NUMERIC (1)
		DECLARE @DataRec_stockid NUMERIC (19)
		DECLARE @DataRec_tableid NUMERIC (1)
		DECLARE @DataRec_f9 NVARCHAR(24)
		DECLARE @DataRec_f8 NVARCHAR(24)
		DECLARE @DataRec_f7 NVARCHAR(24)
		DECLARE @DataRec_f6 NVARCHAR (40)
		DECLARE @DataRec_f22 NVARCHAR (24)
		DECLARE @DataRec_f5 NVARCHAR (40)
		DECLARE @DataRec_f21 NUMERIC (3)
		DECLARE @DataRec_f4 NVARCHAR (40)
		DECLARE @DataRec_f20 FLOAT
		DECLARE @DataRec_f3 NVARCHAR (20)
		DECLARE @DataRec_f2 NVARCHAR (80)
		DECLARE @DataRec_f1 NVARCHAR (24)
		DECLARE @DataRec_imporder NUMERIC (12)
		DECLARE @DataRec_f19 NUMERIC (10)
		DECLARE @DataRec_f18 NUMERIC (3)
		DECLARE @DataRec_f17 NUMERIC (3)
		DECLARE @DataRec_f16 NUMERIC (3)
		DECLARE @DataRec_f15 NUMERIC (3)
		DECLARE @DataRec_f14 NUMERIC (3)
		DECLARE @DataRec_f13 NVARCHAR(24)
		DECLARE @DataRec_f12 NVARCHAR(24)
		DECLARE @DataRec_f11 NVARCHAR(24)
		DECLARE @DataRec_f10 NVARCHAR(24)
		DECLARE @i                      NUMERIC(3) 
		DECLARE @ErrM                                     VARCHAR(2000) 
		DECLARE @ErrId                                    NUMERIC (12) 
		DECLARE @LoopProcessingFlag NUMERIC(1)
		DECLARE @updateSeq int
		DECLARE @StockCardCount as numeric(19)
		DECLARE @startRecord1 int
		DECLARE @startRecord2 int
		DECLARE @skipFailedRecordResult int 
		Declare @StockCode nvarchar(40) 
		Declare @IsRunning int
		Declare	@ControlCount int
		Declare @MaxStockID bigint
		Declare @PluImportValue int
		--DECLARE cdata CURSOR FOR SELECT * FROM IMP_DATA where IMPORDER>=@StartRecord1 oASADASDASDASDASTESTTT IMPORDER

		SELECT @isError = 0 
		SELECT @prevTableID = 0


        select @ControlCount=count(*) from IMP_CONTROL

		if @ControlCount>0 
		BEGIN
			UPDATE  IMP_CONTROL  SET ISRUNNING = 1 , HASDATAFORPCN=0
		END
		ELSE 
		BEGIN
			INSERT INTO IMP_CONTROL values(1,0,0,0,0,0,0)
		END
		SELECT @adv_sql_rowcount = @@ROWCOUNT
		COMMIT TRANSACTION

		BEGIN TRANSACTION
	IF EXISTS(select * from IMP_DATA where processed IN (1,2)  and (tableID=1 or tableID=0))
	--so there has been a processing for this data, the last processed stock card group was a failure, we must skip this one
	BEGIN
		select @StartRecord2 = mAX(IMPORDER) from IMP_DATA where processed IN (1,2)  and (tableID=1	or tableId=0)
		IF EXISTS(SELECT * from IMP_DATA WHERE processed=0 and (tableID=1 or TableID=0))
		BEGIN
		--so  there are more records to be processed?
			select @StartRecord1 =min(IMPORDER) from IMP_DATA WHERE processed=0 and (tableID=1 or TableID=0)
			--update IMP_DATA set PROCESSED=1 where IMPORDER =(select min(IMPORDER) from IMP_DATA where processed IN (1,2)  and tableID=1)
			--delete IMP_DATA where IMPORDER<@StartRecord1
		END
		ELSE
		--so  there are NO more records to be processed?
		BEGIN
			SET @StartRecord1 = 0
		END
	END
	ELSE
	--so there has NOT been a processing for this data
	BEGIN
		SET @StartRecord2 = 0
		IF EXISTS(select * from IMP_DATA where processed=0 and (tableID=1  or TableID=0))
		BEGIN
		--so  there are more records to be processed? USUALLY FIRST TIME RUN should be this one
			select @StartRecord1 = min(IMPORDER) from IMP_DATA where processed=0 and (tableID=1	or TableId=0)
			
		END
		ELSE
		--so  there are NO more records to be processed? Lets get out from this procedure  then!
		BEGIN
			SET @StartRecord1 = 0
		END
	END


	BEGIN
	set @PluImportValue=0

	select @PluImportValue=  VARIABLE_VALUE FROM CONFIG_PARAMETER WHERE VARIABLE_NAME='PLU_IMPORT_LIMIT'

	IF	(@PluImportValue>0)

	BEGIN
	
	SET @MaxStockID=0
	IF EXISTS(select * from IMP_DATA where processed=0 and (tableID=1  or TableID=0))
		BEGIN
		--so  there are more records to be processed? USUALLY FIRST TIME RUN should be this one
			SELECT @MaxStockID=MAX(IMPORDER) FROM
				(select TOP (@PluImportValue)  IMPORDER from IMP_DATA where processed=0 and (tableID=1	or TableId=0))  TBL 	
		END
		ELSE
		--so  there are NO more records to be processed? Lets get out from this procedure  then!
		BEGIN
			SET @MaxStockID = 0
		END
	END	

	 END


COMMIT TRANSACTION	

IF(@PluImportValue>0)

	BEGIN
		DECLARE cdata CURSOR FOR SELECT * FROM IMP_DATA where IMPORDER>=@StartRecord1  AND IMPORDER<=@MaxStockID order by IMPORDER
	END
ELSE
    
	BEGIN
	DECLARE cdata CURSOR FOR SELECT * FROM IMP_DATA where IMPORDER>=@StartRecord1 order by IMPORDER
	END


if (@StartRecord1>0)
BEGIN
		BEGIN TRANSACTION
		
		DELETE FROM STOCK_BARCODE
		WHERE NOT EXISTS
		(	SELECT	1 
			FROM 	STOCK_CARD
			WHERE	ID = STOCK_BARCODE.FK_STOCK_CARD
		)
		
		COMMIT TRANSACTION
		BEGIN TRANSACTION
		-- Update StockID fields
		UPDATE  IMP_DATA   
		SET	STOCKID = stock_card.[ID] 
		FROM  IMP_DATA,
			 stock_card 
		WHERE  IMP_DATA.f1  = stock_card.code
				 AND	(IMP_DATA.TABLEID  = 1 or IMP_DATA.TABLEID =0) AND IMP_DATA.IMPORDER>=@StartRecord1
		COMMIT TRANSACTION
		BEGIN TRANSACTION
		SELECT @adv_sql_rowcount = @@ROWCOUNT 
 		-- Find Sequence Starting Points
		
		SELECT @seqSC  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 1
		 AND	(STOCKID  is NULL OR STOCKID  =0) AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT 

		SELECT @seqSCE  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 2 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT 

		SELECT @seqSB  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 3 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT 

		SELECT @seqSP  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 4 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT 

		SELECT @seqSCP  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 5 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT 

		SELECT @seqSL  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 6 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT

		SELECT @seqSCD  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 7 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT  
		
		SELECT @seqSSP  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 8 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT 
		
		SELECT @seqSS  =  COUNT(1)
		FROM  IMP_DATA 
		WHERE	 TableID  = 9 AND IMP_DATA.IMPORDER>=@StartRecord1
		SELECT @adv_sql_rowcount = @@ROWCOUNT 
		
		

		EXEC GENIUS3.ReturnNextVal 'STOCK_CARD'  , @seqSC  ,  @seqSC OUTPUT 
		EXEC GENIUS3.ReturnNextVal 'STOCK_CARD_EXTENSION'  ,  @seqSCE  ,  @seqSCE OUTPUT
		EXEC GENIUS3.ReturnNextVal 'STOCK_BARCODE'  ,  @seqSB  ,  @seqSB  OUTPUT
		IF @prmWithoutPrice=0
		BEGIN
		EXEC GENIUS3.ReturnNextVal 'STOCK_PRICE'  ,  @seqSP  ,  @seqSP OUTPUT
		END
		EXEC GENIUS3.ReturnNextVal 'STOCK_CARD_PARAMETER'  ,  @seqSCP  ,  @seqSCP OUTPUT
		EXEC GENIUS3.ReturnNextVal 'STOCK_LABEL'  ,  @seqSL  ,  @seqSL OUTPUT
		EXEC GENIUS3.ReturnNextVal 'STOCK_CARD_DETAIL'  ,  @seqSCD  ,  @seqSCD OUTPUT
		EXEC GENIUS3.ReturnNextVal 'STOCK_SUPPLIER'  ,  @seqSSP  ,  @seqSSP OUTPUT
		EXEC GENIUS3.ReturnNextVal 'STOCK_STORE'  ,  @seqSS  ,  @seqSS OUTPUT

		COMMIT TRANSACTION
		BEGIN TRANSACTION

		--temp Variable
		
		SELECT @vvokBarcodeCount  = 0 
		SELECT @vverrBarcodeCount  = 0 
		SELECT @vvokPriceCount  = 0 
		SELECT @vverrPriceCount  = 0 
		-- Main Opeation
		
	

		DECLARE @count		 INT 
		SELECT @count = 1 
		OPEN cData 

		WHILE (0 = 0) 
		BEGIN --( 
			fetch NEXT FROM cData INTO 
				@DataRec_imporder,
				@DataRec_tableid, 		
				@DataRec_stockid, 
				@DataRec_f1,
				@DataRec_f2,
				@DataRec_f3,
				@DataRec_f4,
				@DataRec_f5,
				@DataRec_f6,
				@DataRec_f7,
				@DataRec_f8,
				@DataRec_f9,
				@DataRec_f10,
				@DataRec_f11,
				@DataRec_f12,
				@DataRec_f13,
				@DataRec_f14,
				@DataRec_f15,
				@DataRec_f16,
				@DataRec_f17,
				@DataRec_f18,
				@DataRec_f19,
				@DataRec_f20,
				@DataRec_f21,
				@DataRec_f22,
				@DataRec_processed,
				@updateSeq

			IF (@@FETCH_STATUS = -1) 
			BREAK

			SET @LoopProcessingFlag = 0
			IF @DataRec_tableid = 1 
			BEGIN 
				IF @isError = 1 
				BEGIN  ROLLBACK TRANSACTION
				--	INSERT INTO  IMP_LOG    
				--	 VALUES 		( 0 , 
				--			@ErrId , 
				--			@ErrM , 
				--			1 )  
					INSERT INTO  IMP_LOG ( IMPORDER, TABLEID, EXPLANATION,UPDATESEQ)
					 VALUES 	(@DataRec_imporder , 
							@DataRec_tableid , 
							@ErrM , 
							1 )  	

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
			SELECT @adv_sql_rowcount = @@ROWCOUNT 

					SELECT @isError  = 0 
					
			--		COMMIT TRANSACTION
			--		BEGIN TRANSACTION
				END
				ELSE
				BEGIN 
					UPDATE  IMP_DATA   
					SET	Processed = 1 
					WHERE  F1  = @prevStockCode
					 AND	TableID  = 1 
					

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT 

					
					COMMIT TRANSACTION
					BEGIN TRANSACTION
				END
   
				UPDATE  IMP_CONTROL   
				SET	errBarcodeCount = @vverrBarcodeCount,	
				    okBarcodeCount = @vvokBarcodeCount 
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				
				COMMIT TRANSACTION
				BEGIN TRANSACTION
				UPDATE  IMP_CONTROL   
				SET	ERRPRICECOUNT = @vverrPriceCount,	
				    OKPRICECOUNT = @vvokPriceCount 
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				
				COMMIT TRANSACTION
				BEGIN TRANSACTION
				SET @LoopProcessingFlag = 1
			END
			IF @DataRec_tableid = 3  AND (@LoopProcessingFlag = 0)
			BEGIN 
				IF @isError = 1 
				BEGIN 
					SELECT @vverrBarcodeCount  = @vverrBarcodeCount + 1 
				END
				ELSE
				BEGIN 
					SELECT @vvokBarcodeCount  = @vvokBarcodeCount + 1 
				END
   
				SET @LoopProcessingFlag = 1
			END
			IF @DataRec_tableid = 4  AND (@LoopProcessingFlag = 0)
			BEGIN 
				IF @isError = 1 
				BEGIN 
					SELECT @vverrPriceCount  = @vverrPriceCount + 1 
				END
				ELSE
				BEGIN 
					SELECT @vvokPriceCount  = @vvokPriceCount + 1 
				END
   
			END
   
			SET @LoopProcessingFlag = 0

		select @IsRunning=ISRUNNING from IMP_CONTROL
		
		if @ISRunning=0
			BEGIN
				set @isError=0
				goto exception4
			END

		if @DataRec_tableid = 0
			BEGIN


			IF EXISTS(SELECT 1 FROM STORE WHERE NUM = @DataRec_F2)
				BEGIN
					SELECT @prmSTORE_ID = ID FROM STORE WHERE NUM = @DataRec_F2
				END
			ELSE
				BEGIN
					SET @prmSTORE_ID = 0
				END

				update IMP_DATA set PROCESSED= 1 where IMPORDER=@DataRec_imporder

				--CILEM
				delete from STOCK_STORE
					where FK_STOCK_CARD=@DataRec_stockid and FK_STORE=@prmSTORE_ID

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT  
				--CILEM BITIR

				delete from STOCK_CARD_EXTENSION 
					where FK_STOCK_CARD=@DataRec_stockid

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT 

				delete from STOCK_CARD_PARAMETER 
					where FK_STOCK_CARD=@DataRec_stockid and FK_STORE=@prmSTORE_ID

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT 

				delete from STOCK_LABEL 
					where FK_STOCK_CARD=@DataRec_stockid

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT 
				
				--CILEM
				IF @prmWithoutPrice=0				
				BEGIN
				delete from STOCK_PRICE 
					where FK_STOCK_CARD=@DataRec_stockid and FK_STORE=@prmSTORE_ID

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT 
				END
				--
				delete from STOCK_BARCODE 
					where  FK_STOCK_CARD=@DataRec_stockid

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT

				delete from STOCK_CARD_DETAIL 
					where FK_STOCK_CARD=@DataRec_stockid and FK_STORE=@prmSTORE_ID

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT  

				delete from STOCK_CARD 
					where ID=@DataRec_stockid

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT 
				COMMIT TRANSACTION
				BEGIN TRANSACTION
			END  

			IF @DataRec_tableid = 1 
			BEGIN 
				-- Store Values of StockID and StockCode of New Record
				
				SELECT @prevStockID  = ISNULL(@DataRec_stockid, 0)

		
				SELECT @StockCardCount=COUNT(1) from 
				STOCK_CARD WHERE CODE=@DataRec_f1

				if @StockCardCount>0 
				BEGIN
					SELECT @prevStockID = ID from STOCK_CARD
					where CODE=@DataRec_f1
				END



				SELECT @prevStockCode  = @DataRec_f1 
				SELECT @prevBarcode  = @DataRec_f2 
				SELECT @prevNum  = @DataRec_f3 
				
				-- Get correspondig ID values of Foreign Keys

				Delete from LSTDELETED
				
				SELECT @DataRec_f11  = GENIUS3.GetDeptVAT(@DataRec_f11)
				SELECT @DataRec_f12  = GENIUS3.GetUnitID(@DataRec_f12)
				SELECT @DataRec_f13  = GENIUS3.GetPDGID(@DataRec_f13)
				-- INSERT / UPDATE Real Data into Table Stock_Card
			

				IF @prevStockID = 0 
						
				BEGIN 	
				
				PRINT(	'1')					
					INSERT INTO  STOCK_CARD   
							( ID , 
							CODE , 
							DESCRIPTION , 
							ITEM_TYPE , 
							POS_DESCRIPTION , 

							SHELF_DESCRIPTION , 
							SCALE_DESCRIPTION , 
							UNIT_DIVISOR , 
							UNIT_MULTIPLIER , 
							QUANTITY_FLAG , 
							SCALE_FLAG , 
							FK_DEPARTMENT_VAT , 
							FK_UNIT , 
							FK_PROMO_DISCOUNT_GROUP )  
					 VALUES 		( @seqSC , 
							@DataRec_f1 , 
							@DataRec_f2 , 
							@DataRec_f3 , 
							@DataRec_f4 , 
							@DataRec_f5 , 
							@DataRec_f6 , 
							@DataRec_f7 , 
							@DataRec_f8 , 
							@DataRec_f9 , 
							@DataRec_f10 , 
							@DataRec_f11 , 
							@DataRec_f12 , 
							@DataRec_f13 )  
					

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

					SELECT @prevStockID  = @seqSC 
					SELECT @seqSC  = @seqSC + 1 
				END
				ELSE
				
				
				BEGIN 
					PRINT(@DataRec_f1)
					UPDATE  STOCK_CARD   
					SET	CODE = @DataRec_f1,	
					    DESCRIPTION = @DataRec_f2,	
					    ITEM_TYPE = @DataRec_f3,	
					    POS_DESCRIPTION = @DataRec_f4,	
					    SHELF_DESCRIPTION = @DataRec_f5,	
					    SCALE_DESCRIPTION = @DataRec_f6,	
					    UNIT_DIVISOR = @DataRec_f7,	
					    UNIT_MULTIPLIER = @DataRec_f8,	
					    QUANTITY_FLAG = @DataRec_f9,	
					    SCALE_FLAG = @DataRec_f10,	
					    FK_DEPARTMENT_VAT = @DataRec_f11,	
					    FK_UNIT = @DataRec_f12,	
					    FK_PROMO_DISCOUNT_GROUP = @DataRec_f13 
					WHERE  ID  = @PrevStockID 
					
					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				END
   
				
				DELETE FROM   STOCK_BARCODE    
				WHERE  fk_stock_card  = @prevStockID 
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				DELETE FROM   STOCK_LABEL    
				WHERE  fk_stock_card  = @prevStockID 
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SET @LoopProcessingFlag = 1
			END
			IF @DataRec_tableid = 2 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 
				IF @DataRec_f1 = 0
				BEGIN
					SET @DataRec_f1 = @prmSTORE_NO
				END
				EXECUTE  GENIUS3.GetStoreID @DataRec_f1, @DataRec_f1 OUTPUT
				IF LEN(@DataRec_f22)> 0 and @DataRec_f22 != '0' 
				BEGIN 
					-- Get Linked StockCardID
					
					SELECT @DataRec_f22  =  ID
					FROM  stock_card 
					WHERE	 stock_card.code  = @DataRec_f22
					

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
					BEGIN
						GOTO Exception3
					END
SELECT @adv_sql_rowcount = @@ROWCOUNT 

					GOTO ExitLabel3
					Exception3:
						  
						BEGIN
						
						SELECT @DataRec_f22  = 0 
						/*  Manual Intervention to verify Exception is required */
						END
					ExitLabel3:
				END
   
				DELETE FROM   STOCK_CARD_EXTENSION    
				WHERE  fk_stock_card  = @prevStockID
				 AND	fk_store  = @DataRec_f1 
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				INSERT INTO  STOCK_CARD_EXTENSION   
						( ID , 
						FK_STORE , 
						FK_STOCK_CARD , 
						SCALE_CONTENT1 , 
						SCALE_CONTENT2 , 
						SCALE_CONTENT3 , 
						SCALE_EXPIREDATE , 
						MIN_QUANTITY , 
						MAX_QUANTITY , 
						DISCOUNT_TYPE , 
						DISCOUNT_PERCENT , 
						DISCOUNT_AMOUNT , 
						MAX_DISCOUNT_PERCENT , 
						MAX_DISCOUNT_AMOUNT , 
						PRICE_FLAG , 
						RETURN_FLAG , 
						SELLER_FLAG , 
						CODE_FLAG , 
						DISCOUNT_FLAG , 
						PROPERTY_BITFLAG , 
						POINT , 
						ACTIVE , 
						FK_STOCK_CARD_LINK )  
				 VALUES 		( @seqSCE , 
						@DataRec_f1 , 
						@prevStockID , 
						@DataRec_f3 , 
						@DataRec_f4 , 
						@DataRec_f5 , 
						@DataRec_f6 , 
						@DataRec_f7 , 
						@DataRec_f8 , 
						@DataRec_f9 , 
						@DataRec_f10 , 
						CONVERT(MONEY,@DataRec_f11 ), 
						@DataRec_f12 , 
						CONVERT(MONEY,@DataRec_f13 ),  
						@DataRec_f14 , 
						@DataRec_f15 , 
						@DataRec_f16 , 
						@DataRec_f17 , 
						@DataRec_f18 , 
						@DataRec_f19 , 
						@DataRec_f20 , 
						@DataRec_f21 , 
						ISNULL(@DataRec_f22, 0) )  
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSCE  = @seqSCE + 1 
				SET @LoopProcessingFlag = 1
			END
			IF @DataRec_tableid = 3 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 

				/* DELETE BARCODE IF EXISTS */
				DELETE FROM STOCK_BARCODE
				WHERE BARCODE = @DataRec_f2
				/* DELETE BARCODE IF EXISTS */

				INSERT INTO  STOCK_BARCODE   
						( ID , 
						FK_STOCK_CARD , 
						BARCODE , 
						PTYPE , 
						QUANTITY , 
						CK_STOCK_PRICE_NO,
						BUYING_UNIT,
						PACKAGE_CONTENTS,
						PARAM_3,
						PARAM_4,
						PARAM_5,
						PARAM_6,
						PARAM_7,
						PARAM_8 )  
				 VALUES 		( @seqSB , 
						@prevStockID , 
						@DataRec_f2 , 
						@DataRec_f3 , 
						CONVERT(MONEY, @DataRec_f4) , 
						CONVERT(NUMERIC(19,0), @DataRec_f5),
						@DataRec_f6,
						@DataRec_f7,
						@DataRec_f8,
						@DataRec_f9,
						@DataRec_f10,
						@DataRec_f11,
						@DataRec_f12,
						@DataRec_f13 )  
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSB  = @seqSB + 1 
				SET @LoopProcessingFlag = 1
			END
			IF @prmWithoutPrice=0
			BEGIN
			IF @DataRec_tableid = 4 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 
				IF @DataRec_f1 = 0
				BEGIN
					SET @DataRec_f1 = @prmSTORE_NO
				END
				
				EXECUTE  GENIUS3.GetStoreID @DataRec_f1, @DataRec_f1 OUTPUT
				SELECT @DataRec_f6  = GENIUS3.GetPTypeID(@DataRec_f6)
				--IF not GENIUS3.IsDeleted(4, @DataRec_f1) = 1 
				--created for isdeleted function to procedure conversion

				Execute GENIUS3.IsDeleted 4, @DataRec_f1,@IFound OUTPUT
				IF @IFound = 0
				BEGIN 
					DELETE FROM   STOCK_PRICE    
					WHERE  fk_store  = @DataRec_f1
					 AND	fk_stock_card  = @prevStockID 
					
					INSERT INTO LSTDELETED (ID, Num) Values(4, @DataRec_f1)

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				END
   
				INSERT INTO  STOCK_PRICE   
						( ID , 
						FK_STORE , 
						FK_STOCK_CARD , 
						NUM , 
						UNIT_PRICE , 
						LABEL_PRICE , 
						FK_PAYMENT_TYPE )  
				 VALUES 		( @seqSP , 
						@DataRec_f1 , 
						@prevStockID , 
						@DataRec_f3 , 
						CONVERT(MONEY, @DataRec_f4) , 
						CONVERT(MONEY, @DataRec_f5) , 
						@DataRec_f6 )  
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSP  = @seqSP + 1 
				SET @LoopProcessingFlag = 1
			END
			END
			IF @DataRec_tableid = 5 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 
				IF @DataRec_f1 = 0
				BEGIN
					SET @DataRec_f1 = @prmSTORE_NO
				END

				EXECUTE  GENIUS3.GetStoreID @DataRec_f1, @DataRec_f1 OUTPUT
				--IF not GENIUS3.IsDeleted(5, @DataRec_f1) = 1 
				--created for isdeleted function to procedure conversion
				Execute GENIUS3.IsDeleted 5, @DataRec_f1,@IFound OUTPUT
				IF @IFound = 0 
				BEGIN 
	
					INSERT INTO LSTDELETED (ID, Num) Values(5, @DataRec_f1)

					DELETE FROM   STOCK_CARD_PARAMETER    
					WHERE  FK_STORE  = @DataRec_f1
					 AND	FK_STOCK_CARD  = @prevStockID 
					

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				END
   
				INSERT INTO  STOCK_CARD_PARAMETER   
						( ID , 
						FK_STORE , 
						FK_STOCK_CARD , 
						NUM , 
						PARAM,
						PARAM_1)  
				 VALUES 		( @seqSCP , 
						@DataRec_f1 , 
						@prevStockID , 
						@DataRec_f3 , 
						@DataRec_f4,
						@DataRec_f5)  
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSCP  = @seqSCP + 1 
				SET @LoopProcessingFlag = 1
			END
			IF @DataRec_tableid = 6 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 
				SELECT @DataRec_f4  = GENIUS3.GetUnitID(@DataRec_f4)
				SELECT @DataRec_f6  = GENIUS3.GetUnitID(@DataRec_f6)
				INSERT INTO  STOCK_LABEL   
						( ID , 
						FK_STOCK_CARD , 
						DESCRIPTION , 
						PACKET_AMOUNT , 
						FK_UNIT_PACKET , 
						LABEL_AMOUNT , 
						FK_UNIT_LABEL )  
				 VALUES 		( @seqSL , 
						@prevStockID , 
						@DataRec_f2 , 
						@DataRec_f3 , 
						@DataRec_f4 , 
						@DataRec_f5 , 
						@DataRec_f6 )  
				

				SELECT @adv_error = @@ERROR

				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSL  = @seqSL + 1 
			END
			
			IF @DataRec_tableid = 7 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 
				IF @DataRec_f1 = 0
				BEGIN
					SET @DataRec_f1 = @prmSTORE_NO
				END

				EXECUTE  GENIUS3.GetStoreID @DataRec_f1, @DataRec_f1 OUTPUT
				--IF not GENIUS3.IsDeleted(5, @DataRec_f1) = 1 
				--created for isdeleted function to procedure conversion
				Execute GENIUS3.IsDeleted 7, @DataRec_f1,@IFound OUTPUT
				IF @IFound = 0 
				BEGIN 
	
					INSERT INTO LSTDELETED (ID, Num) Values(7, @DataRec_f1)

					DELETE FROM   STOCK_CARD_DETAIL    
					WHERE  FK_STORE  = @DataRec_f1
					 AND	FK_STOCK_CARD  = @prevStockID 
					

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				END
   
				INSERT INTO  STOCK_CARD_DETAIL   
						( ID , 
						FK_STORE , 
						FK_STOCK_CARD , 
						PTYPE ,
						AMOUNT,
						FLAG, 
						PARAM,
						PARAM_1,
						PARAM_2,
						PARAM_3,
						PARAM_4,
						PARAM_5,
						OPTION_BITFLAG)  
				 VALUES 		( @seqSCD , 
						@DataRec_f1 , 
						@prevStockID , 
						@DataRec_f3 , 
						@DataRec_f4,
						@DataRec_f5,
						@DataRec_f6,
						@DataRec_f7,
						@DataRec_f8,
						@DataRec_f9,
						@DataRec_f10,
						@DataRec_f11,
						@DataRec_f12)  
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSCD  = @seqSCD + 1 
				SET @LoopProcessingFlag = 1
			END
			
			IF @DataRec_tableid = 8 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 
				
				select @DataRec_f1=GENIUS3.GetSupplierID(@DataRec_f1)
				--IF not GENIUS3.IsDeleted(5, @DataRec_f1) = 1 
				--created for isdeleted function to procedure conversion
				Execute GENIUS3.IsDeleted 8, @DataRec_f1,@IFound OUTPUT
				IF @IFound = 0 
				BEGIN 
	
					INSERT INTO LSTDELETED (ID, Num) Values(8, @DataRec_f1)

					DELETE FROM   STOCK_SUPPLIER    
					WHERE  FK_SUPPLIER  = @DataRec_f1
					 AND	FK_STOCK_CARD  = @prevStockID 
					

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
				SELECT @adv_sql_rowcount = @@ROWCOUNT 

				END
   
				if(@DataRec_f1>0)
				BEGIN
				INSERT INTO  STOCK_SUPPLIER  
						( ID , 
						FK_SUPPLIER , 
						FK_STOCK_CARD , 
						BARCODE ,
						CK_STOCK_PRICE_NO)  
				 VALUES 		( @seqSSP , 
						@DataRec_f1 , 
						@prevStockID , 
						@DataRec_f3 , 
						@DataRec_f4)  
				END

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
			SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSSP  = @seqSSP + 1 
				SET @LoopProcessingFlag = 1
			END

			--CILEM
				IF @DataRec_tableid = 9 and @isError = 0  AND (@LoopProcessingFlag = 0)
			BEGIN 
				IF @DataRec_f1 = 0
				BEGIN
					SET @DataRec_f1 = @prmSTORE_NO
				END

				EXECUTE  GENIUS3.GetStoreID @DataRec_f1, @DataRec_f1 OUTPUT
				--IF not GENIUS3.IsDeleted(5, @DataRec_f1) = 1 
				--created for isdeleted function to procedure conversion
				Execute GENIUS3.IsDeleted 9, @DataRec_f1,@IFound OUTPUT
				IF @IFound = 0 
				BEGIN 
	
					INSERT INTO LSTDELETED (ID, Num) Values(9, @DataRec_f1)

					DELETE FROM   STOCK_STORE    
					WHERE  FK_STORE  = @DataRec_f1
					 AND	FK_STOCK_CARD  = @prevStockID 
					

					SELECT @adv_error = @@ERROR
					IF @adv_error != 0 
						GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				END
   
				INSERT INTO  STOCK_STORE   
						(ID
						 ,FK_STORE
						 ,FK_STOCK_CARD
						 ,BARCODE
						 ,OPTION_BITFLAG
						 ,PARAM_1
						 ,CK_STOCK_PRICE_NO
						 )  
				 VALUES 		( @seqSS , 
						@DataRec_f1 , 
						@prevStockID , 
						@DataRec_f3 , 
						@DataRec_f4,
						@DataRec_f5,
						ISNULL(@DataRec_f6, 0) 
						)  
				

				SELECT @adv_error = @@ERROR
				IF @adv_error != 0 
					GOTO Exception2
SELECT @adv_sql_rowcount = @@ROWCOUNT 

				SELECT @seqSS  = @seqSS + 1 
				SET @LoopProcessingFlag = 1
			END
		--CILEM BITIS
			
			SELECT @prevTableID  = @DataRec_tableid 
			GOTO ExitLabel2
			Exception2:
				  
				BEGIN
			
				DECLARE @adv_sqlErrm VARCHAR(4000)
				SELECT @adv_sqlErrm = description from master.dbo.sysmessages where error = @adv_error
				SELECT @ErrM  = @adv_sqlErrm 
				SELECT @ErrID  = @DataRec_imporder 
				SELECT @isError  = 1 
				INSERT INTO  IMP_LOG ( IMPORDER,  EXPLANATION,TABLEID,UPDATESEQ)
					 		VALUES 	(@ErrID  , 
								@ErrM  , 
								@DataRec_tableid , 
								1 )  
				SELECT @adv_sql_rowcount = @@ROWCOUNT 
				END
			ExitLabel2:
			SELECT @count=@count +1
		END --) 
		
		-- Main Data Loop
		Exception4:

		IF @isError = 1 
		BEGIN  
			ROLLBACK TRANSACTION
			INSERT INTO  IMP_LOG   
					( IMPORDER , 
					TABLEID , 
					EXPLANATION , 
					UPDATESEQ )  
			 VALUES 		( 0 , 
					@ErrID , 
					@ErrM , 
					1 )  
			SELECT @adv_sql_rowcount = @@ROWCOUNT 

			
			COMMIT TRANSACTION
			BEGIN TRANSACTION
		END
		ELSE
		BEGIN 
			
			UPDATE  IMP_DATA   
			SET	Processed = 1 
			WHERE  TableID  = 1
			 AND	F1  = @prevStockCode 
			SELECT @adv_sql_rowcount = @@ROWCOUNT 

			
			COMMIT TRANSACTION
			BEGIN TRANSACTION
			UPDATE  IMP_CONTROL   
			SET	okBarcodeCount = okBarcodeCount + 1 
			SELECT @adv_sql_rowcount = @@ROWCOUNT 

			
			COMMIT TRANSACTION
			BEGIN TRANSACTION
			UPDATE  IMP_CONTROL   
			SET	OKPRICECOUNT = OKPRICECOUNT + 1 
			SELECT @adv_sql_rowcount = @@ROWCOUNT 

			
			COMMIT TRANSACTION
			BEGIN TRANSACTION
		END
   
		
		close cData
		DEALLOCATE cdata
		
	
		UPDATE  IMP_CONTROL   
		SET	ISRUNNING = 0 , HASDATAFORPCN=2
		SELECT @adv_sql_rowcount = @@ROWCOUNT 

		
		COMMIT TRANSACTION

		SET NOCOUNT OFF

		SET CONCAT_NULL_YIELDS_NULL ON

	END
		UPDATE  IMP_CONTROL   
		SET	ISRUNNING = 0 , HASDATAFORPCN=2
		SELECT @adv_sql_rowcount = @@ROWCOUNT 
end
