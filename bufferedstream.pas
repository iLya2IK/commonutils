{ BufferedStream
   stream class with non-owned memory buffer
   Copyright (c) 2018-2019 Ilya Medvedkov   }

unit BufferedStream;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils;

type

{ TBufferedStream }

TBufferedStream = class(TCustomMemoryStream)
public
  procedure SetPtr(Ptr: Pointer; ASize: PtrInt);
  function Write(const Buffer; Count: Longint): Longint; override;
end;

implementation

{ TBufferedStream }

procedure TBufferedStream.SetPtr(Ptr: Pointer; ASize: PtrInt);
begin
  SetPointer(Ptr, ASize);
  Seek(0, soFromBeginning);
end;

function TBufferedStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result:=Count;
  If (Count>0) then
  begin
    If (Result>(Size-Position)) then
       Result:=(Size-Position);
    Move(Buffer,(Memory+Position)^,Result);
    Seek(Result, soFromCurrent);
  end;
end;

end.

