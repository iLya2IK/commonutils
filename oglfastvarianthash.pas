{ OGLFastVariantHash
   lightweighted variant hashes
   Copyright (c) 2020-2021 Ilya Medvedkov   }

unit OGLFastVariantHash;

{$mode objfpc}{$H+}

interface

uses
  Variants;

type
  TFastHashItem=record
    HashValue : LongWord;
    NextIndex : Integer;
    Data      : Variant;
  end;
  PFastHashItem=^TFastHashItem;

const
  MaxFastHashListSize = 128;
  MaxFastHashTableSize = 128;
  MaxFastItemsPerHash = 3;

type
  PFastHashItemList = ^TFastHashItemList;
  TFastHashItemList = array[0..MaxFastHashListSize - 1] of TFastHashItem;
  PFastHashTable = ^TFastHashTable;
  TFastHashTable = array[0..MaxFastHashTableSize - 1] of Integer;

  IFastHashList = interface
    Function Get(AName: LongWord): Variant;
    Function GetAtPos(Index: Integer): TFastHashItem;
    Function Add(const AName:LongWord;Item: Variant): Integer;
    function Count: Integer;
    property Items[Index: LongWord]: Variant read Get; default;
    property AtPos[Index: Integer]: TFastHashItem read GetAtPos;
  end;

  { TFastHashList }

  TFastHashList = class(TInterfacedObject, IFastHashList)
  private
    { ItemList }
    FHashList     : PFastHashItemList;
    FCount,
    FCapacity : Integer;
    { Hash }
    FHashTable    : PFastHashTable;
    FHashCapacity : Integer;
    function GetAtPos(Index: Integer): TFastHashItem;
    function GetPtAtPos(Index: Integer): PFastHashItem;
    Function InternalFind(AHash:LongWord;out PrevIndex:Integer):Integer;
  protected
    Function Get(AName: LongWord): Variant;
    Procedure SetCapacity(NewCapacity: Integer);
    Procedure SetCount(NewCount: Integer);
    Procedure AddToHashTable(Index: Integer);
    Procedure SetHashCapacity(NewCapacity: Integer);
    procedure ReHash;
  public
    constructor Create;
    destructor Destroy; override;
    Function Add(const AName:LongWord;Item: Variant): Integer;
    function Count: Integer;
    Procedure Clear;
    Function HashOfIndex(Index: Integer): LongWord;
    Function GetNextCollision(Index: Integer): Integer;
    Procedure Delete(Index: Integer);
    Function Expand: TFastHashList;
    Function Find(const AName:LongWord): Variant;
    Function FindIndexOf(const AName:LongWord): Integer;
    Function GetHashOfValue(const aValue : Variant) : LongWord;
    property Capacity: Integer read FCapacity write SetCapacity;
    property Items[AName: LongWord]: Variant read Get; default;
    property AtPos[Index: Integer]: TFastHashItem read GetAtPos;
    property AtPosPt[Index: Integer]: PFastHashItem read GetPtAtPos;
    property List: PFastHashItemList read FHashList;
  end;

function FastHash(const Vals : Array of Variant) : IFastHashList;
function GenHash(const Vals : Array of Variant) : TFastHashList;

implementation

function FastHash(const Vals : Array of Variant) : IFastHashList;
begin
  Result := GenHash(Vals);
end;

function GenHash(const Vals : Array of Variant) : TFastHashList;
var i : integer;
begin
  if Length(Vals) mod 2 > 0 then Exit(nil);
  Result := TFastHashList.Create;
  i := Low(Vals);
  while i < High(Vals) do
  begin
    if not VarIsOrdinal(Vals[i]) then exit;
    Result.Add(Vals[i], Vals[i+1]);
    Inc(i, 2);
  end;
end;

procedure TFastHashList.AddToHashTable(Index: Integer);
var
  HashIndex : Integer;
begin
  with FHashList^[Index] do
    begin
    HashIndex:=HashValue mod LongWord(FHashCapacity);
    NextIndex:=FHashTable^[HashIndex];
    FHashTable^[HashIndex]:=Index;
    end;
end;

procedure TFastHashList.SetHashCapacity(NewCapacity: Integer);
begin
  if (NewCapacity < 1) then Exit;
  if FHashCapacity=NewCapacity then Exit;
  FHashCapacity:=NewCapacity;
  ReallocMem(FHashTable, FHashCapacity*SizeOf(Integer));
  ReHash;
end;

procedure TFastHashList.ReHash;
var
  i : Integer;
begin
  FillDword(FHashTable^,FHashCapacity,LongWord(-1));
  for i:=0 to FCount-1 do
    AddToHashTable(i);
end;

constructor TFastHashList.Create;
begin
  FHashCapacity := 0;
  SetHashCapacity(1);
end;

destructor TFastHashList.Destroy;
begin
  Clear;
  if Assigned(FHashTable) then
    FreeMem(FHashTable);

  inherited Destroy;
end;

function TFastHashList.Add(const AName: LongWord; Item: Variant): Integer;
begin
  if FCount = FCapacity then
    Expand;
  FillChar(FHashList^[FCount], SizeOf(TFastHashItem), 0);
  with FHashList^[FCount] do
    begin
    HashValue:=AName;
    Data:=Item;
    end;
  AddToHashTable(FCount);
  Result:=FCount;
  Inc(FCount);
end;

function TFastHashList.Count: Integer;
begin
  Result := FCount;
end;

procedure TFastHashList.Clear;
var hptr : PFastHashItem;
begin
  if Assigned(FHashList) then
    begin
    hptr := PFastHashItem(FHashList);
      while FCount > 0 do
      begin
        VarClear(hptr^.Data);
        Inc(hptr);
        Dec(FCount);
      end;
    SetCapacity(0);
    FHashList:=nil;
    end;
  SetHashCapacity(1);
  FHashTable^[0]:=(-1); // sethashcapacity does not always call rehash
end;

function TFastHashList.InternalFind(AHash: LongWord; out PrevIndex: Integer
  ): Integer;
var
  HashIndex : Integer;
begin
  HashIndex:=AHash mod LongWord(FHashCapacity);
  Result:=FHashTable^[HashIndex];
  PrevIndex:=-1;
  while Result<>-1 do
    with FHashList^[Result] do
      begin
      if (HashValue=AHash) then
        Exit;
      PrevIndex:=Result;
      Result:=NextIndex;
      end;
end;

function TFastHashList.GetAtPos(Index: Integer): TFastHashItem;
begin
  Result := FHashList^[Index];
end;

function TFastHashList.GetPtAtPos(Index: Integer): PFastHashItem;
begin
  Result := @(FHashList^[Index]);
end;

function TFastHashList.Get(AName: LongWord): Variant;
begin
  Result:=Find(AName);
end;

function TFastHashList.HashOfIndex(Index: Integer): LongWord;
begin
  If (Index < 0) or (Index >= FCount) then Exit(LongWord(-1));
  Result:=FHashList^[Index].HashValue;
end;

function TFastHashList.GetNextCollision(Index: Integer): Integer;
begin
  Result:=-1;
  if ((Index > -1) and (Index < FCount)) then
    Result:=FHashList^[Index].NextIndex;
end;

procedure TFastHashList.Delete(Index: Integer);
begin
  if (Index<0) or (Index>=FCount) then Exit;
  { Remove from HashList }
  Dec(FCount);
  System.Move(FHashList^[Index+1], FHashList^[Index], (FCount - Index) * SizeOf(TFastHashItem));
  { All indexes are updated, we need to build the hashtable again }
  ReHash;
  { Shrink the list if appropriate }
  if (FCapacity > 256) and (FCount < FCapacity shr 2) then
    begin
    FCapacity:=FCapacity shr 1;
    ReAllocMem(FHashList, SizeOf(TFastHashItem) * FCapacity);
    end;
end;

function TFastHashList.Expand: TFastHashList;
var
  IncSize : Longint;
begin
  Result:=Self;
  if FCount < FCapacity then
    Exit;
  IncSize:=SizeOf(PtrInt)*2;
  if FCapacity > 127 then
    Inc(IncSize, FCapacity shr 2)
  else if FCapacity > SizeOf(PtrInt)*3 then
    Inc(IncSize, FCapacity shr 1)
  else if FCapacity >= SizeOf(PtrInt) then
    Inc(IncSize,sizeof(PtrInt));
  SetCapacity(FCapacity + IncSize);
end;

function TFastHashList.Find(const AName: LongWord): Variant;
var
  Index,
  PrevIndex : Integer;
begin
  Index:=InternalFind(AName,PrevIndex);
  if Index=-1 then
    Exit(Null);
  Result:=FHashList^[Index].Data;
end;

function TFastHashList.FindIndexOf(const AName: LongWord): Integer;
var
  PrevIndex : Integer;
begin
  Result:=InternalFind(AName,PrevIndex);
end;

function TFastHashList.GetHashOfValue(const aValue: Variant): LongWord;
var i : integer;
  hptr : PFastHashItem;
begin
  Result := LongWord(-1);

  i := FCount;
  hptr := PFastHashItem(FHashList);
  while i > 0 do
  begin
    if VarCompareValue(hptr^.Data, aValue) = vrEqual then
    begin
      Exit(hptr^.HashValue);
    end;

    Inc(hptr);
    Dec(i);
  end;
end;

procedure TFastHashList.SetCapacity(NewCapacity: Integer);
begin
  if (NewCapacity < FCount) or (NewCapacity > MaxFastHashListSize) then Exit;
  if NewCapacity = FCapacity then
    Exit;
  ReallocMem(FHashList, NewCapacity*SizeOf(TFastHashItem));
  FCapacity:=NewCapacity;
  { Maybe expand hash also }
  if FCapacity>FHashCapacity*MaxFastItemsPerHash then
    SetHashCapacity(FCapacity div MaxFastItemsPerHash);
end;

procedure TFastHashList.SetCount(NewCount: Integer);
begin
  if (NewCount < 0) or (NewCount > MaxFastHashListSize)then Exit;
  if NewCount > FCount then
    begin
    if NewCount > FCapacity then
      SetCapacity(NewCount);
    if FCount < NewCount then
      FillChar(FHashList^[FCount], (NewCount-FCount) div SizeOf(TFastHashItem), 0);
    end;
  FCount:=NewCount;
end;


end.

