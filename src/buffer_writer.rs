use crate::bson;
use crate::utils::{BufferSlice, IntoOk, PageAddress};
use std::convert::Infallible;
use std::iter::Once;

pub struct BufferWriter<'a, I> {
    slices: I,
    current: Option<&'a mut BufferSlice>,
    position_in_slice: usize,
    global_position: usize,
}

impl<'a> BufferWriter<'a, Once<&'a mut BufferSlice>> {
    pub fn single(slice: &'a mut BufferSlice) -> Self {
        Self::fragmented(std::iter::once(slice))
    }
}

impl<'a, I: Iterator<Item = &'a mut BufferSlice>> BufferWriter<'a, I> {
    pub fn fragmented(slices: impl IntoIterator<IntoIter = I>) -> Self {
        let mut slices = slices.into_iter();
        let current = slices.next();
        Self {
            slices,
            current,
            position_in_slice: 0,
            global_position: 0,
        }
    }

    pub fn write_document(&mut self, document: &bson::Document) {
        document.write_value(self).into_ok1();
    }

    pub(crate) fn write_array(&mut self, array: &bson::Array) {
        array.write_value(self).into_ok1();
    }
}

trait BufferOrSize: Copy {
    fn len(self) -> usize;
    fn is_empty(self) -> bool {
        self.len() == 0
    }
    fn split_at(self, mid: usize) -> (Self, Self);
    fn data(&self) -> Option<&[u8]>;
}

impl BufferOrSize for usize {
    fn len(self) -> usize {
        self
    }

    fn split_at(self, mid: usize) -> (Self, Self) {
        (mid, self - mid)
    }

    fn data(&self) -> Option<&[u8]> {
        None
    }
}

impl BufferOrSize for &[u8] {
    fn len(self) -> usize {
        self.len()
    }

    fn split_at(self, mid: usize) -> (Self, Self) {
        self.split_at(mid)
    }

    fn data(&self) -> Option<&[u8]> {
        Some(self)
    }
}

impl<'a, I: Iterator<Item = &'a mut BufferSlice>> BufferWriter<'a, I> {
    fn write_skip(&mut self, mut data: impl BufferOrSize) {
        while !data.is_empty() {
            let current = self.current.as_mut().expect("End of Stream");

            let current_remaining = current.len() - self.position_in_slice;

            if data.len() < current_remaining {
                // we can write data in current slice
                if let Some(data) = data.data() {
                    current.write_bytes(self.position_in_slice, data);
                }
                self.position_in_slice += data.len();
                self.global_position += data.len();
                assert!(self.position_in_slice > 0 && self.position_in_slice <= current.len());
                return;
            } else {
                // we use current slice fully
                let (to_current, next) = data.split_at(current_remaining);
                if let Some(to_current) = to_current.data() {
                    current.write_bytes(self.position_in_slice, to_current);
                }
                self.global_position += current_remaining;
                data = next;

                self.current = self.slices.next();
                self.position_in_slice = 0;
            }
        }
    }

    pub fn skip(&mut self, bytes: usize) {
        self.write_skip(bytes);
    }

    pub fn position(&self) -> usize {
        self.global_position
    }
}

#[allow(dead_code)]
impl<'a, I: Iterator<Item = &'a mut BufferSlice>> BufferWriter<'a, I> {
    fn write(&mut self, data: &[u8]) {
        self.write_skip(data);
    }

    pub fn write_i32(&mut self, value: i32) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u32(&mut self, value: u32) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u16(&mut self, value: u16) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u8(&mut self, value: u8) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_i8(&mut self, value: i8) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_i64(&mut self, value: i64) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u64(&mut self, value: u64) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_f64(&mut self, value: f64) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_bool(&mut self, value: bool) {
        self.write_u8(value as u8);
    }

    pub fn write_cstring(&mut self, value: &str) {
        debug_assert!(value.as_bytes().iter().all(|x| *x != 0));
        self.write(value.as_bytes());
        self.write(&[0]);
    }

    pub fn write_bytes(&mut self, value: &[u8]) {
        self.write(value);
    }

    pub fn write_page_address(&mut self, value: PageAddress) {
        self.write_u32(value.page_id());
        self.write_u8(value.index());
    }
}

impl<'a, I: Iterator<Item = &'a mut BufferSlice>> bson::BsonWriter for BufferWriter<'a, I> {
    type Error = Infallible;

    fn when_too_large(size: usize) -> Self::Error {
        panic!("The content size too long ({} bytes)", size);
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.write_bytes(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::bson::Value;
    use crate::buffer_reader::BufferReader;
    use crate::buffer_writer::BufferWriter;
    use crate::utils::BufferSlice;

    #[test]
    fn buffer_write_cstring() {
        let doc = document! {
          "_id" => 5i64,
          "unique_id" => "20-133-5",
          "event_log" => array![
            document! {
              "created" => 2020-05-06,
              "type" => "job_created"
            },
            document! {
              "created" => date![2020-05-06 09:29:19.0510000],
              "type" => "asset_added",
              "data" => document!{
                "filename" => array!["IMG_1333.JPG"],
                "filepath" => array!["D:\\Users\\Daniel\\Desktop\\German Shepherd\\IMG_1333.JPG"]
              }
            },
            document! {
              "created" => date![2020-05-06 09:29:23.6910000],
              "type" => "lookup_preformed",
              "data" => document!{
                "searchterm" => array!["1424101.2"]
              }
            },
            document! {
              "created" => date![2020-05-06 09:29:25.9060000],
              "type" => "lookup_selected"
            },
            document! {
              "created" => date![2020-05-06 09:29:43.7350000],
              "type" => "job_saved"
            },
            document! {
              "created" => date![2020-05-06 09:29:43.7900000],
              "type" => "job_closed"
            },
            document! {
              "created" => date![2020-06-10 16:00:30.3950000],
              "type" => "job_deleted"
            },
            document! {
              "created" => date![2020-06-10 16:00:30.3950000],
              "type" => "job_deleted"
            },
            document! {
              "created" => date![2020-06-10 16:00:30.3950000],
              "type" => "job_deleted"
            },
            document! {
              "created" => date![2020-06-10 16:00:30.3950000],
              "type" => "job_deleted"
            }
          ],
          "status" => "PERMANANTDELETE",
          "cleaned_up" => false,
          "user_info" => document!{
            "href" => "/fotoweb/users/dan%40deathstar.local",
            "userName" => "dan@deathstar.local",
            "fullName" => "DanTwomey",
            "firstName" => "Dan",
            "lastName" => "Twomey",
            "email" => "dan@medialogix.co.uk",
            "userId" => "15003",
            "isGuest" => "false",
            "userAvatarHref" => "https://www.gravatar.com/avatar/9496065924d90ffa6b6184c741aa0184?d=mm"
          },
          "device_info" => document!{
            "_id" => Value::Null,
            "short_id" => 133,
            "device_name" => "DANSCOMPUTER"
          },
          "template_id" => "5cb0b82fd1654e07c7a3dd72",
          "created" => date![2020-05-06 09:29:10.8350000],
          "last_save" => date![2020-06-15 19:40:50.8250000],
          "files" => array![
            document! {
              "_id" => "5f9bffbc-a6d7-4ccb-985b-17470745f760",
              "filename" => "IMG_1333.JPG",
              "extension" => ".JPG",
              "file_checksum" => "SHA1:09025C2C3009051C51877E052A740140F73EC518",
              "local_file_info" => document!{
                "imported_datetime" => date![2020-05-06 09:29:17.7650000],
                "system_created_datetime" => date![2020-03-26 17:04:08.9930000],
                "original_file_path" => "D:\\Users\\Daniel\\Desktop\\German Shepherd\\IMG_1333.JPG",
                "local_file_path" => "C:\\ProgramData\\Medialogix\\Pixel\\UploadStorage\\20-133-5\\5f9bffbc-a6d7-4ccb-985b-17470745f760\\IMG_1333.JPG",
                "original_file_directory" => "D:\\Users\\Daniel\\Desktop\\German Shepherd",
                "thumbnail_path" => "C:\\ProgramData\\Medialogix\\Pixel\\UploadStorage\\20-133-5\\5f9bffbc-a6d7-4ccb-985b-17470745f760\\IMG_1333.JPG.thumb"
              },
              "filesize_bytes" => 4225974i64,
              "friendly_filesize" => "4MB",
              "metadata" => document!{
                "2c0066d2-3f9f-4cf8-8d06-33a544624418" => Value::Null,
                "4a389ee1-9e1b-4e06-b46f-23f1fd8f6a93" => Value::Null,
                "b0ad5374-213f-488f-bb21-407e782de287" => Value::Null,
                "91328cc4-eb72-4c30-9545-e931c830e847" => Value::Null,
                "b94b21cf-eef3-4e8c-951a-1c20d16d871f" => Value::Null,
                "3a660b33-c99f-4111-ba88-633533017b40" => Value::Null,
                "500c2388-ccc1-4b63-8da1-5bbb468a0c5b" => Value::Null,
                "652cdabe-3c6f-4765-86fd-1680749b412b" => Value::Null,
                "2a2668c3-2b69-4f9b-89a8-914b70e00aa3" => Value::Null,
                "fd67fdb2-3705-4f14-a929-5336c8e46489" => Value::Null,
                "2405d44c-13d3-4ce3-8ba1-dae189139f84" => array![],
                "8b73f206-8b2c-4ce5-9867-a4e1892370e5" => Value::Null,
                "5c73f206-8b2c-4ce5-9852-a4e1892370a5" => array!["csitemplate"],
                "9fc32696-4efd-4b6a-8fcc-554c75421cff" => array!["{{asset.uploadtype}}"],
                "c47645ab-0bfa-42e0-9c43-66868f10f90f" => array!["{{curentuser.username}}"],
                "a16a3bae-59bc-4583-9015-7f6bbd0d2b87" => array!["{{job.id}}"]
              },
              "status" => "CREATED",
              "file_valid" => false,
              "type" => "IMAGE",
              "fotoweb_responses" => array![]
            }
          ],
          "lookup_metadata" => document!{
            "2c0066d2-3f9f-4cf8-8d06-33a544624418" => array!["1424101.2"],
            "4a389ee1-9e1b-4e06-b46f-23f1fd8f6a93" => array!["Exhibit 2"],
            "b0ad5374-213f-488f-bb21-407e782de287" => array!["1424101.2 - Exhibit 2"],
            "91328cc4-eb72-4c30-9545-e931c830e847" => array!["Location 3"],
            "b94b21cf-eef3-4e8c-951a-1c20d16d871f" => array!["DHL"],
            "3a660b33-c99f-4111-ba88-633533017b40" => array!["Medium"]
          },
          "error_reason" => Value::Null,
          "retry_count" => 0,
          "error_counters" => document!{},
          "deleted_datetime" => date![2020-06-10 16:00:30.3920000],
          "delete_when" => date![2020-06-15 16:00:30.3920000]
        };

        let mut buf0 = [0u8; 2935];
        let mut buf1 = [0u8; 97];
        let mut buf2 = [0u8; 5];
        let mut buf3 = [0u8; 189];
        let arr0 = BufferSlice::new_mut(&mut buf0);
        let arr1 = BufferSlice::new_mut(&mut buf1);
        let arr2 = BufferSlice::new_mut(&mut buf2);
        let arr3 = BufferSlice::new_mut(&mut buf3);

        let mut writer = BufferWriter::fragmented([arr0, arr1, arr2, arr3]);
        writer.write_document(&doc);

        drop(writer);

        let arr0 = BufferSlice::new(&buf0);
        let arr1 = BufferSlice::new(&buf1);
        let arr2 = BufferSlice::new(&buf2);
        let arr3 = BufferSlice::new(&buf3);
        let mut reader = BufferReader::fragmented([arr0, arr1, arr2, arr3]);
        let read = reader.read_document().unwrap();

        assert_eq!(doc, read);
    }

    #[test]
    fn buffer_write_cstring_basic() {
        let mut arr0 = [0; 3];
        let mut arr1 = [0; 4];
        let mut arr2 = [0; 5];
        let mut arr3 = [0; 6];
        let mut arr4 = [0; 7];

        let slice0 = BufferSlice::new_mut(&mut arr0);
        let slice1 = BufferSlice::new_mut(&mut arr1);
        let slice2 = BufferSlice::new_mut(&mut arr2);
        let slice3 = BufferSlice::new_mut(&mut arr3);
        let slice4 = BufferSlice::new_mut(&mut arr4);

        let mut writer = BufferWriter::fragmented([slice0, slice1, slice2, slice3, slice4]);
        writer.write_cstring("123456789*ABCEFGHIJ");
        writer.write_cstring("abc");

        drop(writer);

        let slice0 = BufferSlice::new(&mut arr0);
        let slice1 = BufferSlice::new(&mut arr1);
        let slice2 = BufferSlice::new(&mut arr2);
        let slice3 = BufferSlice::new(&mut arr3);
        let slice4 = BufferSlice::new(&mut arr4);

        let mut reader = BufferReader::fragmented([slice0, slice1, slice2, slice3, slice4]);
        assert_eq!(reader.read_cstring().unwrap(), "123456789*ABCEFGHIJ");
        assert_eq!(reader.read_cstring().unwrap(), "abc");
    }

    #[test]
    fn buffer_write_numbers() {
        let mut array = [0; 1000];
        let slice = BufferSlice::new_mut(&mut array);
        let mut writer = BufferWriter::fragmented([slice]);

        // max values
        writer.write_i32(i32::MAX);
        writer.write_u32(u32::MAX);
        writer.write_i64(i64::MAX);
        writer.write_f64(f64::MAX);

        // min values
        writer.write_i32(i32::MIN);
        writer.write_u32(u32::MIN);
        writer.write_i64(i64::MIN);
        writer.write_f64(f64::MIN);

        // zero values
        writer.write_i32(0); // int
        writer.write_u32(0); // uint
        writer.write_i64(0); // long
        writer.write_f64(0.0); // double

        // fixed values
        writer.write_i32(1990); // int
        writer.write_u32(1990); // uint
        writer.write_i64(1990); // long
        writer.write_f64(1990.0); // double

        drop(writer);

        let slice = BufferSlice::new(&array);

        let mut p = 0;
        assert_eq!(slice.read_i32(p), i32::MAX);
        p += 4;
        assert_eq!(slice.read_u32(p), u32::MAX);
        p += 4;
        assert_eq!(slice.read_i64(p), i64::MAX);
        p += 8;
        assert_eq!(slice.read_f64(p), f64::MAX);
        p += 8;

        assert_eq!(slice.read_i32(p), i32::MIN);
        p += 4;
        assert_eq!(slice.read_u32(p), u32::MIN);
        p += 4;
        assert_eq!(slice.read_i64(p), i64::MIN);
        p += 8;
        assert_eq!(slice.read_f64(p), f64::MIN);
        p += 8;

        assert_eq!(slice.read_i32(p), 0);
        p += 4;
        assert_eq!(slice.read_u32(p), 0);
        p += 4;
        assert_eq!(slice.read_i64(p), 0);
        p += 8;
        assert_eq!(slice.read_f64(p), 0.0);
        p += 8;

        assert_eq!(slice.read_i32(p), 1990);
        p += 4;
        assert_eq!(slice.read_u32(p), 1990);
        p += 4;
        assert_eq!(slice.read_i64(p), 1990);
        p += 8;
        assert_eq!(slice.read_f64(p), 1990.0);
        p += 8;

        _ = p;
    }
}
