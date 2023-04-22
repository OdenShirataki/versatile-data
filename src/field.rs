use idx_sized::{Avltriee, IdxSized, Removed};
use std::{cmp::Ordering, io, path::Path};
use various_data_file::VariousDataFile;

pub mod entity;
use entity::FieldEntity;

pub struct FieldData {
    index: IdxSized<FieldEntity>,
    data_file: VariousDataFile,
}
impl FieldData {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        Ok(FieldData {
            index: IdxSized::new({
                let mut path = path.to_path_buf();
                path.push(".i");
                path
            })?,
            data_file: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.push(".d");
                path
            })?,
        })
    }
    pub fn entity(&self, row: u32) -> Option<&FieldEntity> {
        if let Ok(max_rows) = self.index.max_rows() {
            if max_rows >= row {
                return unsafe { self.index.triee().value(row) };
            }
        }
        None
    }
    pub fn get<'a>(&self, row: u32) -> Option<&'a [u8]> {
        if let Some(e) = self.entity(row) {
            Some(unsafe {
                std::slice::from_raw_parts(
                    self.data_file.offset(e.addr() as isize) as *const u8,
                    e.len() as usize,
                )
            })
        } else {
            None
        }
    }
    pub fn num(&self, row: u32) -> Option<f64> {
        if let Some(e) = self.entity(row) {
            Some(e.num())
        } else {
            None
        }
    }
    pub fn index(&self) -> &IdxSized<FieldEntity> {
        &self.index
    }
    pub fn triee(&self) -> &Avltriee<FieldEntity> {
        &self.index.triee()
    }
    pub fn update(&mut self, row: u32, content: &[u8]) -> io::Result<u32> {
        if let Some(org) = self.index.value(row) {
            if unsafe { self.data_file.bytes(org.data_address()) } == content {
                return Ok(row);
            }
            //変更がある場合はまず消去
            if let Removed::Last(data) = self.index.delete(row) {
                self.data_file.remove(&data.data_address())?; //削除対象がユニークの場合は対象文字列を完全削除
            }
        }
        //TODO:handle unwrap
        let cont_str = std::str::from_utf8(content).unwrap();
        let tree = self.index.triee();
        let (ord, found_row) = tree.search_cb(|data| -> Ordering {
            let bytes = unsafe { self.data_file.bytes(data.data_address()) };
            if content == bytes {
                Ordering::Equal
            } else {
                //TODO:handle unwrap
                natord::compare(cont_str, std::str::from_utf8(bytes).unwrap())
            }
        });
        if ord == Ordering::Equal && found_row != 0 {
            self.index.insert_same(found_row, row)
        } else {
            //新しく作る
            let data_address = self.data_file.insert(content)?;
            let e = FieldEntity::new(data_address.address(), cont_str.parse().unwrap_or(0.0));
            if let Some(_entity) = unsafe { self.index.triee().node(row) } {
                //既存データの更新処理
                unsafe {
                    self.index.triee_mut().update_node(found_row, row, e, ord);
                }
                Ok(row)
            } else {
                //追加
                self.index.insert_unique(e, found_row, ord, row)
            }
        }
    }
    pub fn delete(&mut self, row: u32) {
        self.index.delete(row);
    }

    pub(crate) fn search_cb(&self, cont: &[u8]) -> (Ordering, u32) {
        self.index.triee().search_cb(|data| -> Ordering {
            let str2 = unsafe {
                std::slice::from_raw_parts(
                    self.data_file.offset(data.addr() as isize) as *const u8,
                    data.len() as usize,
                )
            };
            if cont == str2 {
                Ordering::Equal
            } else {
                //TODO:handle unwrap
                natord::compare(
                    std::str::from_utf8(cont).unwrap(),
                    std::str::from_utf8(str2).unwrap(),
                )
            }
        })
    }
}

#[test]
fn test() {
    let dir = "./vd-test-fd/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();
    if let Ok(mut fd) = FieldData::new(&(dir.to_owned() + "test")) {
        //1
        fd.update(1, b"Noah").unwrap();
        fd.update(2, b"Liam").unwrap();
        fd.update(3, b"Olivia").unwrap();
        fd.update(1, b"Renamed Noah").unwrap();
        fd.update(2, b"Renamed Liam").unwrap();
        fd.update(3, b"Renamed Olivia").unwrap();

        //2
        fd.update(4, b"Noah").unwrap();
        fd.update(5, b"Liam").unwrap();
        fd.update(6, b"Olivia").unwrap();
        fd.update(1, b"Renamed Renamed Noah").unwrap();
        fd.update(2, b"Renamed Renamed Liam").unwrap();
        fd.update(3, b"Renamed Renamed Olivia").unwrap();
        fd.update(4, b"Renamed Noah").unwrap();
        fd.update(5, b"Renamed Liam").unwrap();
        fd.update(6, b"Renamed Olivia").unwrap();

        //3
        fd.update(7, b"Noah").unwrap();
        fd.update(8, b"Liam").unwrap();
        fd.update(9, b"Olivia").unwrap();
        fd.update(1, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(2, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(3, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(4, b"Renamed Renamed Noah").unwrap();
        fd.update(5, b"Renamed Renamed Liam").unwrap();
        fd.update(6, b"Renamed Renamed Olivia").unwrap();
        fd.update(7, b"Renamed Noah").unwrap();
        fd.update(8, b"Renamed Liam").unwrap();
        fd.update(9, b"Renamed Olivia").unwrap();

        //4
        fd.update(10, b"Noah").unwrap();
        fd.update(11, b"Liam").unwrap();
        fd.update(12, b"Olivia").unwrap();
        fd.update(1, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(2, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(3, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(4, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(5, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(6, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(7, b"Renamed Renamed Noah").unwrap();
        fd.update(8, b"Renamed Renamed Liam").unwrap();
        fd.update(9, b"Renamed Renamed Olivia").unwrap();
        fd.update(10, b"Renamed Noah").unwrap();
        fd.update(11, b"Renamed Liam").unwrap();
        fd.update(12, b"Renamed Olivia").unwrap();

        //5
        fd.update(13, b"Noah").unwrap();
        fd.update(14, b"Liam").unwrap();
        fd.update(15, b"Olivia").unwrap();
        fd.update(1, b"Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(2, b"Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(3, b"Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(4, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(5, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(6, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(7, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(8, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(9, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(10, b"Renamed Renamed Noah").unwrap();
        fd.update(11, b"Renamed Renamed Liam").unwrap();
        fd.update(12, b"Renamed Renamed Olivia").unwrap();
        fd.update(13, b"Renamed Noah").unwrap();
        fd.update(14, b"Renamed Liam").unwrap();
        fd.update(15, b"Renamed Olivia").unwrap();

        //6
        fd.update(16, b"Noah").unwrap();
        fd.update(17, b"Liam").unwrap();
        fd.update(18, b"Olivia").unwrap();
        fd.update(1, b"Renamed Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(2, b"Renamed Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(3, b"Renamed Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(4, b"Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(5, b"Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(6, b"Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(7, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(8, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(9, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(10, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(11, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(12, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(13, b"Renamed Renamed Noah").unwrap();
        fd.update(14, b"Renamed Renamed Liam").unwrap();
        fd.update(15, b"Renamed Renamed Olivia").unwrap();
        fd.update(16, b"Renamed Noah").unwrap();
        fd.update(17, b"Renamed Liam").unwrap();
        fd.update(18, b"Renamed Olivia").unwrap();

        //7
        fd.update(19, b"Noah").unwrap();
        fd.update(20, b"Liam").unwrap();
        fd.update(21, b"Olivia").unwrap();
        fd.update(
            1,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            2,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            3,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(4, b"Renamed Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(5, b"Renamed Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(6, b"Renamed Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(7, b"Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(8, b"Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(9, b"Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(10, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(11, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(12, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(13, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(14, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(15, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(16, b"Renamed Renamed Noah").unwrap();
        fd.update(17, b"Renamed Renamed Liam").unwrap();
        fd.update(18, b"Renamed Renamed Olivia").unwrap();
        fd.update(19, b"Renamed Noah").unwrap();
        fd.update(20, b"Renamed Liam").unwrap();
        fd.update(21, b"Renamed Olivia").unwrap();

        //8
        fd.update(22, b"Noah").unwrap();
        fd.update(23, b"Liam").unwrap();
        fd.update(24, b"Olivia").unwrap();
        fd.update(
            1,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            2,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            3,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(
            4,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            5,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            6,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(7, b"Renamed Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(8, b"Renamed Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(9, b"Renamed Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(10, b"Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(11, b"Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(12, b"Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(13, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(14, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(15, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(16, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(17, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(18, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(19, b"Renamed Renamed Noah").unwrap();
        fd.update(20, b"Renamed Renamed Liam").unwrap();
        fd.update(21, b"Renamed Renamed Olivia").unwrap();
        fd.update(22, b"Renamed Noah").unwrap();
        fd.update(23, b"Renamed Liam").unwrap();
        fd.update(24, b"Renamed Olivia").unwrap();

        //9
        fd.update(25, b"Noah").unwrap();
        fd.update(26, b"Liam").unwrap();
        fd.update(27, b"Olivia").unwrap();
        fd.update(
            1,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            2,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            3,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(
            4,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            5,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            6,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(
            7,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            8,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            9,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(10, b"Renamed Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(11, b"Renamed Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(
            12,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(13, b"Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(14, b"Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(15, b"Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(16, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(17, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(18, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(19, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(20, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(21, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(22, b"Renamed Renamed Noah").unwrap();
        fd.update(23, b"Renamed Renamed Liam").unwrap();
        fd.update(24, b"Renamed Renamed Olivia").unwrap();
        fd.update(25, b"Renamed Noah").unwrap();
        fd.update(26, b"Renamed Liam").unwrap();
        fd.update(27, b"Renamed Olivia").unwrap();

        //10
        fd.update(28, b"Noah").unwrap();
        fd.update(29, b"Liam").unwrap();
        fd.update(30, b"Olivia").unwrap();
        fd.update(
            1,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            2,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(3,b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia").unwrap();
        fd.update(
            4,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            5,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            6,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(
            7,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            8,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            9,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(
            10,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            11,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            12,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(13, b"Renamed Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(14, b"Renamed Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(
            15,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(16, b"Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(17, b"Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(18, b"Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(19, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(20, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(21, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(22, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(23, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(24, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(25, b"Renamed Renamed Noah").unwrap();
        fd.update(26, b"Renamed Renamed Liam").unwrap();
        fd.update(27, b"Renamed Renamed Olivia").unwrap();
        fd.update(28, b"Renamed Noah").unwrap();
        fd.update(29, b"Renamed Liam").unwrap();
        fd.update(30, b"Renamed Olivia").unwrap();

        //11
        fd.update(31, b"Noah").unwrap();
        fd.update(32, b"Liam").unwrap();
        fd.update(33, b"Olivia").unwrap();
        fd.update(1,b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah").unwrap();
        fd.update(2,b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam").unwrap();
        fd.update(3,b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia").unwrap();
        fd.update(
            4,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            5,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(6,b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia").unwrap();
        fd.update(
            7,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            8,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            9,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(
            10,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            11,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            12,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(
            13,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Noah",
        )
        .unwrap();
        fd.update(
            14,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Liam",
        )
        .unwrap();
        fd.update(
            15,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(16, b"Renamed Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(17, b"Renamed Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(
            18,
            b"Renamed Renamed Renamed Renamed Renamed Renamed Olivia",
        )
        .unwrap();
        fd.update(19, b"Renamed Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(20, b"Renamed Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(21, b"Renamed Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(22, b"Renamed Renamed Renamed Renamed Noah")
            .unwrap();
        fd.update(23, b"Renamed Renamed Renamed Renamed Liam")
            .unwrap();
        fd.update(24, b"Renamed Renamed Renamed Renamed Olivia")
            .unwrap();
        fd.update(25, b"Renamed Renamed Renamed Noah").unwrap();
        fd.update(26, b"Renamed Renamed Renamed Liam").unwrap();
        fd.update(27, b"Renamed Renamed Renamed Olivia").unwrap();
        fd.update(28, b"Renamed Renamed Noah").unwrap();
        fd.update(29, b"Renamed Renamed Liam").unwrap();
        fd.update(30, b"Renamed Renamed Olivia").unwrap();
        fd.update(31, b"Renamed Noah").unwrap();
        fd.update(32, b"Renamed Liam").unwrap();
        fd.update(33, b"Renamed Olivia").unwrap();
    }
}
